from luigi_dependency_graphs import (
    save_task_dependencies_diagram,
    save_task_namespace_dependencies_diagram,
)
from sample_pipeline import CalcStats


def main():
    task = CalcStats()
    save_task_namespace_dependencies_diagram(
        task=task, save_path="./data/graphs/TaskDependenciesDiagram.png"
    )
    save_task_dependencies_diagram(task=task, save_dir="./data/graphs/")


if __name__ == "__main__":
    main()
