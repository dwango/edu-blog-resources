# %% 必要なライブラリのインポート
import numpyro
import numpyro.distributions as dist
from numpyro.infer import MCMC, NUTS, Predictive

# Set working device to CPU
numpyro.set_platform("cpu")
numpyro.set_host_device_count(3)

import jax.numpy as jnp
from jax.random import PRNGKey

import matplotlib.pyplot as plt


# %% IRTモデルの定義
def model(X=None, N=None, J=None):
    if N is None or J is None:
        N, J = X.shape
    with numpyro.plate("students", N):
        theta = numpyro.sample("theta", dist.Normal(0, 1))
    with numpyro.plate("items", J):
        b = numpyro.sample("b", dist.Normal(0, 1))
    a = numpyro.sample("a", dist.LogNormal(0.15, 0.60))

    plogits = a * (theta[:, None] - b[None, :])

    if X is not None:
        mask = jnp.isnan(X)
    else:
        mask = jnp.ones((N, J), dtype=bool)

    with numpyro.handlers.mask(mask=~mask):
        with numpyro.plate_stack("obs", (N, J)):
            numpyro.sample("obs", dist.BernoulliLogits(plogits), obs=X)


# %% 人工データの生成
prior_predictive = Predictive(model, num_samples=1)
X = prior_predictive(PRNGKey(0), N=1000, J=30)["obs"][0]
print(X)

# %% サンプラーの指定（基本）
kernel = NUTS(model)

# %% サンプラーの指定（初期値あり）
kernel = NUTS(
    model,
    init_strategy=numpyro.infer.init_to_value(
        values={"a": 0.5, "b": jnp.zeros(30), "theta": jnp.zeros(1000)}
    ),
)

# %% MCMCインスタンスの作成
mcmc = MCMC(
    kernel, num_warmup=500, num_samples=1500, num_chains=3, chain_method="vectorized"
)

# %% MCMCサンプリングの実行
mcmc.run(PRNGKey(0), X=X)

# %% サンプリング結果の確認
mcmc.print_summary()

# %% ArviZでの事後予測分布サンプリング
predictive = Predictive(model, posterior_samples=mcmc.get_samples())
posterior_samples = predictive(PRNGKey(1), X=None, N=1000, J=30)

# %% ArviZでのInferenceDataオブジェクトの作成
import arviz as az

idata = az.from_numpyro(mcmc, posterior_predictive=posterior_samples)

# %% トレースプロットの描画
az.plot_trace(idata, var_names=["a"])
az.plot_trace(idata, var_names=["b"], coords={"b_dim_0": [0]})
az.plot_trace(idata, var_names=["theta"], coords={"theta_dim_0": [0]})

# %% 自己相関の確認
az.plot_autocorr(idata, var_names=["a"], max_lag=20, combined=True)

# %% 間引きありのMCMC実行
kernel = NUTS(model)
mcmc_thin10 = MCMC(
    kernel,
    num_warmup=500,
    num_samples=15000,
    num_chains=3,
    chain_method="parallel",
    thinning=10,
    progress_bar=True,
)
mcmc_thin10.run(PRNGKey(0), X=X)
az.plot_autocorr(
    mcmc_thin10, var_names=["a"], combined=True, figsize=(10, 5), max_lag=20
)

# %% フォレストプロットの描画
az.plot_forest(
    idata, var_names=["a", "b"], combined=True, figsize=(8, 8), hdi_prob=0.94
)

# %% 事後予測モデルチェック
az.plot_ppc(idata, var_names=["obs"], figsize=(10, 5), coords={"obs_dim_1": [0]})

# %% ベイズ的p値の計算とプロット
az.plot_bpv(
    idata, var_names=["obs"], coords={"obs_dim_1": [0]}, kind="t_stat", t_stat="mean"
)


# %% 多群IRTモデルの定義
def model_multiplegroup(X=None, N=None, J=None, G=None, design_matrix=None):
    if N is None or J is None:
        N, J = X.shape
    if G is None:
        Warning("Group indicator vector, G, is not specified.")

    with numpyro.plate("Hyperparameters", np.unique(G).size - 1):
        _mu_theta = numpyro.sample("_mu_theta", dist.Normal(0.0, 1.0))
        _sigma_theta = numpyro.sample("_sigma_theta", dist.HalfNormal(1.0))
    mu_theta = jnp.insert(_mu_theta, 0, 0.0)
    sigma_theta = jnp.insert(_sigma_theta, 0, 1.0)
    mu_theta = numpyro.deterministic("mu_theta", mu_theta)
    sigma_theta = numpyro.deterministic("sigma_theta", sigma_theta)

    with numpyro.plate("students", N):
        theta = numpyro.sample("theta", dist.Normal(mu_theta[G], sigma_theta[G]))

    with numpyro.plate("items", J):
        b = numpyro.sample("b", dist.Normal(0, 1))
    a = numpyro.sample("a", dist.LogNormal(0.15, 0.60))

    plogits = a * (theta[:, None] - b[None, :])

    if X is not None:
        mask = ~jnp.isnan(X)
    elif design_matrix is not None:
        mask = design_matrix
    else:
        mask = jnp.ones((N, J), dtype=bool)
    with numpyro.handlers.mask(mask=mask):
        with numpyro.plate_stack("obs", (N, J)):
            numpyro.sample("obs", dist.BernoulliLogits(plogits), obs=X)


# %% 多群IRTモデルのデータ準備と実行
import numpy as np

G = jnp.concatenate([jnp.full(500, i) for i in range(2)])
design_matrix = np.block(
    [
        [np.ones((500, 15)), np.ones((500, 15)), np.zeros((500, 15))],
        [np.zeros((500, 15)), np.ones((500, 15)), np.ones((500, 15))],
    ]
).astype(bool)

kernel = NUTS(model_multiplegroup)
predict = Predictive(model_multiplegroup, num_samples=1)
predictive_samples = predict(PRNGKey(0), X=None, N=1000, J=45, G=G)
X = predictive_samples["obs"][0]
X = jnp.where(design_matrix, X, jnp.nan)

mcmc = MCMC(
    kernel,
    num_warmup=500,
    num_samples=1500,
    num_chains=3,
    chain_method="vectorized",
    progress_bar=True,
)
mcmc.run(PRNGKey(0), X=X, N=1000, J=45, G=G)

# %% 多群IRTモデルの結果可視化（mu_theta, sigma_theta）
idata = az.from_numpyro(mcmc)

true_mu_theta = predictive_samples["mu_theta"][0]
true_sigma_theta = predictive_samples["sigma_theta"][0]

fig, ax = plt.subplots(figsize=(8, 3))
az.plot_forest(
    idata, var_names=["mu_theta", "sigma_theta"], combined=True, hdi_prob=0.94, ax=ax
)

y_ticks = ax.get_yticks()
for i, true_val in enumerate(reversed(true_sigma_theta)):
    y_pos = y_ticks[i]
    ax.plot(true_val, y_pos, "ro", markersize=8, alpha=0.8)

for i, true_val in enumerate(reversed(true_mu_theta)):
    y_pos = y_ticks[i + len(true_sigma_theta)]
    ax.plot(true_val, y_pos, "ro", markersize=8, alpha=0.8)

plt.title("Posterior estimates vs True values")

# %% 多群IRTモデルの結果可視化（a, b）
fig, ax = plt.subplots(figsize=(8, 12))
az.plot_forest(idata, var_names=["a", "b"], combined=True, hdi_prob=0.94, ax=ax)

true_a = predictive_samples["a"][0]
true_b = predictive_samples["b"][0]

y_ticks = ax.get_yticks()
for i, true_val in enumerate(reversed(true_b)):
    y_pos = y_ticks[i]
    ax.plot(true_val, y_pos, "ro", markersize=6, alpha=0.8)

y_pos = y_ticks[len(true_b)]
ax.plot(true_a, y_pos, "ro", markersize=8, alpha=0.8)

plt.title("Discrimination and Difficulty: Posterior estimates vs True values")


# %% 固定項目パラメタ法のモデル定義
def model_fixed(
    X=None, N=None, J=None, fixed_a=None, fixed_b: None | np.ndarray = None
):
    if N is None or J is None:
        N, J = X.shape

    if fixed_b is not None:
        mu_theta = numpyro.sample("mu_theta", dist.Normal(0.0, 1.0))
        sigma_theta = numpyro.sample("sigma_theta", dist.HalfNormal(1.0))
    else:
        mu_theta = numpyro.deterministic("mu_theta", 0.0)
        sigma_theta = numpyro.deterministic("sigma_theta", 1.0)

    with numpyro.plate("students", N):
        theta = numpyro.sample("theta", dist.Normal(mu_theta, sigma_theta))

    if fixed_a is not None:
        # a is fixed to the provided value
        a = fixed_a
        numpyro.deterministic("a", a)
    else:
        a = numpyro.sample("a", dist.LogNormal(0.15, 0.60))

    if fixed_b is not None:
        nan_mask = np.isnan(fixed_b)
        n_free = np.sum(nan_mask)

        if n_free > 0:
            with numpyro.plate("free", n_free):
                b_free = numpyro.sample("b_free", dist.Normal(0, 1))
            b = jnp.full_like(fixed_b, 0.0)
            b = b.at[nan_mask].set(b_free)
            b = b.at[~nan_mask].set(fixed_b[~nan_mask])
        else:
            b = fixed_b
        numpyro.deterministic("b", b)
    else:
        with numpyro.plate("items", J):
            b = numpyro.sample("b", dist.Normal(0, 1))
    if X is not None:
        mask = ~jnp.isnan(X)
    else:
        mask = jnp.ones((N, J), dtype=bool)

    plogits = a * (theta[:, None] - b[None, :])
    with numpyro.handlers.mask(mask=mask):
        with numpyro.plate_stack("obs", (N, J)):
            numpyro.sample("obs", dist.BernoulliLogits(plogits), obs=X)


# %% 固定項目パラメタ法の実行
X1 = X[G == 0, :30]
kernel = NUTS(model)
mcmc_first = MCMC(
    kernel,
    num_warmup=500,
    num_samples=1500,
    num_chains=3,
    chain_method="vectorized",
    progress_bar=True,
)
mcmc_first.run(PRNGKey(0), X=X1, N=X1.shape[0], J=X1.shape[1])

eap_a = mcmc_first.get_samples()["a"].mean(axis=0)
eap_b = mcmc_first.get_samples()["b"].mean(axis=0)

fixed_b = jnp.concatenate([eap_b, jnp.full(15, jnp.nan)])[15:]

X2 = X[G == 1, 15:]
kernel_fixed = NUTS(model_fixed)
mcmc_fixed = MCMC(
    kernel_fixed,
    num_warmup=500,
    num_samples=1500,
    num_chains=3,
    chain_method="vectorized",
    progress_bar=True,
)
mcmc_fixed.run(
    PRNGKey(0), X=X2, N=X2.shape[0], J=X2.shape[1], fixed_a=eap_a, fixed_b=fixed_b
)

# %% 固定項目パラメタ法の結果可視化
idata_fixed = az.from_numpyro(mcmc_fixed)

fig, ax = plt.subplots(figsize=(8, 12))
az.plot_forest(
    idata_fixed,
    var_names=["b", "mu_theta", "sigma_theta"],
    combined=True,
    hdi_prob=0.94,
    ax=ax,
)

true_b_all = predictive_samples["b"][0][15:]
true_mu_theta_group1 = predictive_samples["_mu_theta"][0][0]
true_sigma_theta_group1 = predictive_samples["_sigma_theta"][0][0]

y_ticks = ax.get_yticks()

ax.plot(true_sigma_theta_group1, y_ticks[0], "ro", markersize=8, alpha=0.8)
ax.plot(true_mu_theta_group1, y_ticks[1], "ro", markersize=8, alpha=0.8)
for i, true_val in enumerate(reversed(true_b_all)):
    y_pos = y_ticks[i + 2]
    ax.plot(true_val, y_pos, "ro", markersize=6, alpha=0.8)

plt.title("Fixed model estimates vs True values (Group 1)\nRed dots show true values")
