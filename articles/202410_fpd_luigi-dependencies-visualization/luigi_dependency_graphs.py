import textwrap
from collections import defaultdict
from dataclasses import dataclass
from os import linesep
from pathlib import Path
from typing import Callable, Type

import graphviz
import luigi


@dataclass(order=True, frozen=True)
class TaskInfo:
    """
    タスク情報を保持するデータクラス
    """

    namespace: str
    name: str
    docstring: str


def extract_class_docstring(class_: Type) -> str:
    """
    extract_class_docstring クラスのdocstringを抽出し、左詰に整形して返す。

    Args:
        class_ (Type): docstring抽出対象のクラス

    Returns:
        str: 左詰に整形されたクラスのdocstring。docstringが存在しない場合は空の文字列。
    """
    if class_.__doc__ is None:
        return ""
    return textwrap.dedent(class_.__doc__).strip()


def get_task_dependencies(task: luigi.Task) -> dict[TaskInfo, list[TaskInfo]]:
    """
    get_task_dependencies タスクの依存関係の辞書（キー：luigiタスクの情報、値：そのタスクの依存しているタスクの情報についてのリスト）を返す。

    Args:
        task (luigi.Task): 依存関係を調べたいタスク

    Returns:
        dict[TaskInfo, list[TaskInfo]]: タスクの依存関係の辞書
    """
    dependencies = defaultdict(list)
    visited = set()

    def _get_task_dependencies(task):
        if task in visited:
            return
        visited.add(task)

        docstring = extract_class_docstring(task.__class__)
        task_info = TaskInfo(task.task_namespace, task.__class__.__name__, docstring)
        dependencies[task_info]

        for req in luigi.task.flatten(task.requires()):
            req_docstring = extract_class_docstring(req.__class__)
            req_task_info = TaskInfo(
                req.task_namespace, req.__class__.__name__, req_docstring
            )
            dependencies[task_info].append(req_task_info)
            _get_task_dependencies(req)

    _get_task_dependencies(task)

    uniquified_dependencies = {k: sorted(set(v)) for k, v in dependencies.items()}
    return uniquified_dependencies


def get_task_namespace_dependencies(task: luigi.Task) -> dict[str, list[str]]:
    """
    get_task_namespace_dependencies タスクの名前空間（task_namespace）の依存関係の辞書（キー：luigiタスクの名前空間、値：その名前空間の依存している名前空間のリスト）を返す。

    Args:
        task (luigi.Task): 依存関係を調べたいタスク

    Returns:
        dict[str, list[str]]: タスクの名前空間（task_namespace）の依存関係の辞書
    """
    dependencies = defaultdict(list)
    visited = set()

    def _get_task_namespace_dependencies(task):
        if task in visited:
            return
        visited.add(task)

        dependencies[task.task_namespace]
        for req in luigi.task.flatten(task.requires()):
            if task.task_namespace != req.task_namespace:
                # 自分自身への依存は含まない
                dependencies[task.task_namespace].append(req.task_namespace)

            _get_task_namespace_dependencies(req)

    _get_task_namespace_dependencies(task)

    uniquified_dependencies = {k: sorted(set(v)) for k, v in dependencies.items()}
    return uniquified_dependencies


def extract_summary_and_details(docstring: str) -> tuple[str, str]:
    """
    docstringからサマリーと詳細を抽出する。

    Args:
        docstring (str): サマリーと詳細の抽出対象のdocstring

    Returns:
        tuple[str, str]: サマリー・詳細
    """
    lines = docstring.strip().splitlines()
    summary = lines[0].strip() if lines else ""
    details = linesep.join(lines[1:]).strip() if len(lines) > 1 else ""

    return summary, details


def format_task(task: TaskInfo) -> str:
    """
    graphviz表示用にluigi.Taskのクラス定義から抽出したタスク情報を整形する。

    Args:
        task (TaskInfo): luigi.Taskのクラス定義から抽出したタスク情報

    Returns:
        str: 整形されたタスク情報についての文字列
    """
    if task.docstring == "":
        return f"{task.namespace}.{task.name}"
    summary, _ = extract_summary_and_details(task.docstring)
    return f"{task.namespace}.{task.name}{linesep}{linesep}{summary}"


def save_task_dependencies_diagram(
    task: luigi.Task,
    save_dir: str | Path,
    output_format: str = "png",
    info_format_function: Callable[[TaskInfo], str] = format_task,
) -> None:
    """
    Luigiタスクの依存関係をGraphvizを用いて図として保存する。

    この関数は、指定されたLuigiタスクに基づいて、タスク間の依存関係を解析し、
    Graphvizを使用して依存関係を視覚化する図を生成する。
    生成された図は、指定されたディレクトリに保存される。

    Args:
        task (luigi.Task): 依存関係を視覚化する対象のLuigiタスク
        save_dir (str | Path):  図を保存するディレクトリのパス。ディレクトリが存在しない場合には新規に作成される。
        output_format (str, optional): 図の出力形式。対応する形式（例: "pdf", "svg"）を指定可能。
        info_format_function (Callable[[TaskInfo], str], optional): `TaskInfo`からノード情報を生成する関数
    """
    _save_dir = Path(save_dir)
    _save_dir.mkdir(exist_ok=True, parents=True)

    task_dependencies = get_task_dependencies(task=task)

    for task_namespace in {i.namespace for i in task_dependencies.keys()}:
        internal_graph_data = {
            k: v for k, v in task_dependencies.items() if k.namespace == task_namespace
        }
        dot = graphviz.Digraph()
        dot.attr(label=task_namespace)
        dot.attr("node", shape="rectangle", style="filled", fillcolor="lightblue")
        for node in internal_graph_data:
            dot.node(info_format_function(node))
        for node, edges in internal_graph_data.items():
            for edge in edges:
                dot.edge(info_format_function(edge), info_format_function(node))

        save_path = Path(
            _save_dir, task.__class__.__name__, f"{task_namespace}.{output_format}"
        )
        Path(_save_dir, task.__class__.__name__).mkdir(exist_ok=True, parents=True)
        dot.render(str(save_path.with_suffix("")), format=output_format)


def save_task_namespace_dependencies_diagram(
    task: luigi.Task, save_path: str | Path
) -> None:
    """
     Luigiタスクの名前空間依存関係をGraphvizを用いて図として保存する。

    この関数は、指定されたLuigiタスクに基づいて、タスク間の名前空間依存関係を解析し、
    Graphvizを使用して依存関係を視覚化する図を生成する。
    生成された図は、指定されたファイルパスに保存される。

    Args:
        task (luigi.Task): 依存関係を調べる対象のLuigiタスク
        save_path (str | Path): 図を保存するファイルのパス。拡張子（例: ".png", ".pdf"）を含める必要がある。

    Raises:
        ValueError: 保存先のパスに拡張子が含まれていない場合に発生
    """
    _save_path = Path(save_path)
    if not _save_path.suffix:
        raise ValueError(f"save_path: {save_path}に拡張子が含まれていません。")

    ext = _save_path.suffix.lstrip(".")
    task_namespace_dependencies = get_task_namespace_dependencies(task=task)

    group_dot = graphviz.Digraph()
    group_dot.attr(label="Task Group Dependencies Diagram")
    group_dot.attr("node", shape="rectangle", style="filled", fillcolor="lightblue")
    for node in task_namespace_dependencies:
        group_dot.node(node)
    for node, edges in task_namespace_dependencies.items():
        for edge in edges:
            group_dot.edge(edge, node)
    group_dot.render(str(_save_path.with_suffix("")), format=ext)
