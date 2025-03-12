from datetime import date, timedelta

import numpy as np
import pandas as pd


def get_timespan(
    df: pd.DataFrame, dt: date, minus: int, periods: int, freq: str = "D"
) -> pd.DataFrame:
    return df[
        pd.date_range(dt - timedelta(days=minus), periods=periods, freq=freq)
    ]


def prepare_dataset(
    df: pd.DataFrame,
    promo_df: pd.DataFrame,
    t2017: date,
    is_train: bool = True,
    name_prefix: str = None,
):
    X: dict[str, pd.DataFrame] = {
        "promo_14_2017": get_timespan(promo_df, t2017, 14, 14)
        .sum(axis=1)
        .values,
        "promo_60_2017": get_timespan(promo_df, t2017, 60, 60)
        .sum(axis=1)
        .values,
        "promo_140_2017": get_timespan(promo_df, t2017, 140, 140)
        .sum(axis=1)
        .values,
        "promo_3_2017_aft": get_timespan(
            promo_df, t2017 + timedelta(days=16), 15, 3
        )
        .sum(axis=1)
        .values,
        "promo_7_2017_aft": get_timespan(
            promo_df, t2017 + timedelta(days=16), 15, 7
        )
        .sum(axis=1)
        .values,
        "promo_14_2017_aft": get_timespan(
            promo_df, t2017 + timedelta(days=16), 15, 14
        )
        .sum(axis=1)
        .values,
    }

    for i in [3, 7, 14, 30, 60, 140]:
        tmp1 = get_timespan(df, t2017, i, i)
        tmp2 = (get_timespan(promo_df, t2017, i, i) > 0) * 1

        X["has_promo_mean_%s" % i] = (
            (tmp1 * tmp2.replace(0, np.nan)).mean(axis=1).values
        )
        X[f"has_promo_mean_{i}_decay"] = (
            (
                tmp1
                * tmp2.replace(0, np.nan)
                * np.power(0.9, np.arange(i)[::-1])
            )
            .sum(axis=1)
            .values
        )

        X["no_promo_mean_%s" % i] = (
            (tmp1 * (1 - tmp2).replace(0, np.nan)).mean(axis=1).values
        )
        X["no_promo_mean_%s_decay" % i] = (
            (
                tmp1
                * (1 - tmp2).replace(0, np.nan)
                * np.power(0.9, np.arange(i)[::-1])
            )
            .sum(axis=1)
            .values
        )

    for i in [3, 7, 14, 30, 60, 140]:
        tmp = get_timespan(df, t2017, i, i)
        X["diff_%s_mean" % i] = tmp.diff(axis=1).mean(axis=1).values
        X["mean_%s_decay" % i] = (
            (tmp * np.power(0.9, np.arange(i)[::-1])).sum(axis=1).values
        )
        X["mean_%s" % i] = tmp.mean(axis=1).values
        X["median_%s" % i] = tmp.median(axis=1).values
        X["min_%s" % i] = tmp.min(axis=1).values
        X["max_%s" % i] = tmp.max(axis=1).values
        X["std_%s" % i] = tmp.std(axis=1).values

    for i in [3, 7, 14, 30, 60, 140]:
        tmp = get_timespan(df, t2017 + timedelta(days=-7), i, i)
        X["diff_%s_mean_2" % i] = tmp.diff(axis=1).mean(axis=1).values
        X["mean_%s_decay_2" % i] = (
            (tmp * np.power(0.9, np.arange(i)[::-1])).sum(axis=1).values
        )
        X["mean_%s_2" % i] = tmp.mean(axis=1).values
        X["median_%s_2" % i] = tmp.median(axis=1).values
        X["min_%s_2" % i] = tmp.min(axis=1).values
        X["max_%s_2" % i] = tmp.max(axis=1).values
        X["std_%s_2" % i] = tmp.std(axis=1).values

    for i in [7, 14, 30, 60, 140]:
        tmp = get_timespan(df, t2017, i, i)
        X["has_sales_days_in_last_%s" % i] = (tmp > 0).sum(axis=1).values
        X["last_has_sales_day_in_last_%s" % i] = (
            i - ((tmp > 0) * np.arange(i)).max(axis=1).values
        )
        X["first_has_sales_day_in_last_%s" % i] = (
            ((tmp > 0) * np.arange(i, 0, -1)).max(axis=1).values
        )

        tmp = get_timespan(promo_df, t2017, i, i)
        X["has_promo_days_in_last_%s" % i] = (tmp > 0).sum(axis=1).values
        X["last_has_promo_day_in_last_%s" % i] = (
            i - ((tmp > 0) * np.arange(i)).max(axis=1).values
        )
        X["first_has_promo_day_in_last_%s" % i] = (
            ((tmp > 0) * np.arange(i, 0, -1)).max(axis=1).values
        )

    tmp = get_timespan(promo_df, t2017 + timedelta(days=16), 15, 15)
    X["has_promo_days_in_after_15_days"] = (tmp > 0).sum(axis=1).values
    X["last_has_promo_day_in_after_15_days"] = (
        i - ((tmp > 0) * np.arange(15)).max(axis=1).values
    )
    X["first_has_promo_day_in_after_15_days"] = (
        ((tmp > 0) * np.arange(15, 0, -1)).max(axis=1).values
    )

    for i in range(1, 16):
        X["day_%s_2017" % i] = get_timespan(df, t2017, i, 1).values.ravel()

    for i in range(7):
        X["mean_4_dow{}_2017".format(i)] = (
            get_timespan(df, t2017, 28 - i, 4, freq="7D").mean(axis=1).values
        )
        X["mean_20_dow{}_2017".format(i)] = (
            get_timespan(df, t2017, 140 - i, 20, freq="7D").mean(axis=1).values
        )

    for i in range(-16, 16):
        X["promo_{}".format(i)] = promo_df[
            t2017 + timedelta(days=i)
        ].values.astype(np.uint8)

    X = pd.DataFrame(X)

    if is_train:
        y = df[pd.date_range(t2017, periods=16)].values
        return X, y
    if name_prefix is not None:
        X.columns = ["%s_%s" % (name_prefix, c) for c in X.columns]
    return X
