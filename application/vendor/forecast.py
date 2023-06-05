import warnings
import numpy as np
# import pmdarima as pm
from prophet import Prophet
import pandas as pd
import datetime as dt
from dateutil.relativedelta import relativedelta
from sklearn.ensemble import RandomForestRegressor
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from statsmodels.tsa.holtwinters import (
    SimpleExpSmoothing,
    ExponentialSmoothing,
)
from application.core.errors import FuncRaiseError
from application.core.base import (
    get_run_date,
    get_process_date,
)
from application.core.utils.logging_ import get_logger

logger = get_logger(__name__)
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore', ConvergenceWarning)


def resample(df, id_value, data_date):
    """
    Parameters
    ----------
    df: DataFrame
        DataFrame with the following columns: ['id', 'ds', 'y']

    id_value: str
        ID to filter from the DataFrame

    data_date: str
        Date string of the data end date (beginning of month), formatted as `%Y-%m-%d` (ISO-format).
        For data that ends in Nov -> '2020-11-01'

    Returns
    -------
    DataFrame
        Ordered DateTime indexed dataframe with all the dates until the forecast start date.
    """

    df = df.loc[df['id'] == id_value]

    # Fill every day until just before forecast start date
    start_date = df['ds'].min()

    # Get end of month
    end_date = dt.date.fromisoformat(data_date) + relativedelta(months=1, days=-1)
    if start_date is None or start_date > end_date or len(df) == 0:
        df_blank = pd.DataFrame(columns=['y'])
        df_blank.index.name = 'ds'
        return df_blank

    date_range = pd.date_range(start_date, end_date, freq='D')
    df_res = pd.DataFrame({'ds': date_range})

    # Join the values and resample to a freqency of interest (in this case monthly)
    df_res = pd.merge(df_res, df, how='left', on='ds')
    df_res.fillna(0, inplace=True)
    return df_res.resample('MS', on='ds')[['y']].sum()


def get_fcst_date_idx(series_y, period):
    """
    Generates the forecast date index based on the resampled DateTime
    indexed series

    Parameters
    ----------
    series_y: Series
        Resampled DateTime indexed series

    period: int
        Forecast periods

    Returns
    -------
    A new set of forecast DateTime index
    """

    date_index = series_y.index
    return pd.date_range(
        date_index[-1],
        periods=period + 1,
        freq=date_index.freq,
    )[1:]


def val_to_gr(series_y, periods=12):
    """
    Parameters
    ----------
    series_y: Series
        Resampled series of an id with ordered DateTime index.
        Index name is `ds` and series name is `y`

    periods: int, default=12
        Number of periods to shift to calculate growth

    Returns
    -------
    Series
        Growth value series with DateTime index.
        Series name is `growth`
    """

    series_gr = (
        series_y.pct_change(periods)
            .iloc[periods:]
            .replace([np.inf, -np.inf, None, np.nan], [1, -1, 1, 1])
    )
    series_gr.name = 'growth'
    return series_gr


def gr_to_val(series_y, series_gr, periods=12):
    """
    Parameters
    ----------
    series_y: Series
        Original resampled series of an id (before calculating growth) with ordered DateTime index.
        Index name is `ds` and series name is `y`

    series_gr: Series
        Growth value series with DateTime index.
        Series name is `growth`

    periods: int, default=12
        Number of periods to shift from which to calculate growth

    Returns
    -------
    Series
        Nominal value of the same DateTime index that results
        from previous_y + previous_y*growth
    """

    series_gr_shift = series_gr.copy()
    series_gr_shift.index = series_gr_shift.index.shift(-periods)

    df_val = pd.merge(
        series_gr_shift,
        series_y,
        how='left',
        left_index=True,
        right_index=True
    )
    series_val = df_val['y'] + df_val['y'] * df_val['growth']
    series_val.index = series_val.index.shift(periods)
    return series_val


def extract_feat(series_y, lag=3, _dropna: bool = True):
    max_date_series_y = max(series_y.index)

    df_y_feat = series_y.copy()
    df_y_feat = pd.DataFrame(df_y_feat)

    for l in range(1, lag + 1):
        df_y_feat[f't-{l}'] = df_y_feat['y'].shift(l)

    df_y_feat['month'] = df_y_feat.index.month
    df_y_feat['quarter'] = df_y_feat.index.quarter
    df_y_feat['year'] = df_y_feat.index.year

    df_y_feat = pd.get_dummies(
        df_y_feat, columns=['month', 'quarter'], drop_first=False
    )
    df_y_feat.dropna(inplace=_dropna)

    df_x = df_y_feat.loc[:, [i for i in df_y_feat.columns if i != 'y']]
    df_y = df_y_feat.loc[:, 'y']

    return df_y_feat, df_x, df_y


# Forecasting models and functions ---------------------------------------------
class DefaultModel:
    def __init__(self, series_y):
        self.series_y = series_y

    def fit(self):
        return self

    def forecast(self, period):
        return pd.Series(dtype=np.float64)


class MovingAverage:
    def __init__(self, series_y):
        self.series_y = series_y

    def fit(self, method=None, window=3):

        if method == 'weighted':
            w = np.arange(1, window + 1)
            self.ma = (
                self.series_y
                    .rolling(window=window, min_periods=1)
                    .apply(
                        lambda x: (
                                np.dot(x, w[-len(x):])
                                / w[-len(x):].sum()
                        )
                    )
                    .iloc[-1]
            )
        elif method == 'exponential':
            self.ma = (
                self.series_y
                    .ewm(span=window, min_periods=1)
                    .mean()
                    .iloc[-1]
            )
        else:
            self.ma = (
                self.series_y
                    .rolling(window=window, min_periods=1)
                    .mean()
                    .iloc[-1]
            )

        return self

    def forecast(self, period):
        date_index_new = get_fcst_date_idx(self.series_y, period)

        ma = pd.Series(data=[self.ma] * period, index=date_index_new)
        ma.name = 'forecast'
        return ma


class Arima:
    def __init__(self, series_y, auto=False, growth=False, periods=12):
        """
        series_y: Series

        auto: bool, default=False
            Whether to use auto arima or not

        growth: bool, default=False
            Whether to convert series_y to growth first then convert
            the results back to nominal

        periods: int
            Periods to calculate growth
        """

        self.auto = auto
        self.growth = growth
        self.periods = periods
        self.series_y = series_y

        if growth:
            order_param = (4, 0, 4)
            self.series_y_forecast = val_to_gr(series_y, periods=periods)
        else:
            order_param = (4, 1, 4)
            self.series_y_forecast = series_y

        if not auto:
            self.m = SARIMAX(
                self.series_y_forecast,
                order=order_param,
                initialization='approximate_diffuse'
            )

    def fit(self):
        # if self.auto:
        #     # try:
        #     #     self.m = pm.arima.auto_arima(self.series_y_forecast, m=12, simple_differencing=True)
        #     # except:
        #     #     self.m = pm.arima.auto_arima(self.series_y_forecast, m=1, simple_differencing=True)
        self.m = self.m.fit(disp=False)

        return self

    def forecast(self, period):
        # if self.auto:
        #     predictions = self.m.predict(period)
        #     date_index = get_fcst_date_idx(self.series_y, period)
        #     fcst_value = pd.Series(data=predictions, index=date_index)
        fcst_value = self.m.forecast(period)

        if self.growth:
            fcst_value.name = 'growth'
            fcst_value = gr_to_val(self.series_y, fcst_value, self.periods)

        return fcst_value


class RandomForest:
    def __init__(self, series_y, growth=False, periods=12):
        self.series_y = series_y
        self.growth = growth
        self.periods = periods
        self.series_y_growth = val_to_gr(series_y, periods=periods)
        self.series_y_growth.name = 'y'

        self.max_date_series_y = max(series_y.index)

    def fit(
            self,
            n_estimators=100,
            min_samples_split=6,
            min_samples_leaf=3,
            max_depth=None,
            max_features='auto',
            random_state=1
    ):
        self.m = RandomForestRegressor(
            n_estimators=n_estimators,
            min_samples_split=min_samples_split,
            min_samples_leaf=min_samples_leaf,
            max_depth=max_depth,
            max_features=max_features,
            random_state=random_state
        )

        # Generate features and fit ...
        if self.growth:
            (
                self.df_y_feat, self.df_x, self.y
            ) = extract_feat(self.series_y_growth)
        else:
            (
                self.df_y_feat, self.df_x, self.y
            ) = extract_feat(self.series_y)

        self.m.fit(self.df_x, self.y)

        return self

    def forecast(self, period):
        # Generate future data points
        fcst_index = get_fcst_date_idx(self.series_y, period)
        self.future_series = pd.Series(data=[np.nan] * period, index=fcst_index)
        self.future_series.name = 'y'
        if self.growth:
            self.total_series = pd.concat([self.series_y_growth, self.future_series])
        else:
            self.total_series = pd.concat([self.series_y, self.future_series])

        self.total_series.name = 'y'

        # Generate future features (concatenated with the original one)
        # and predict..
        for i in range(period, 0, -1):
            (
                self.df_y_feat_future,
                self.df_x_future,
                self.y_future
            ) = extract_feat(self.total_series, _dropna=False)
            self.df_x_future = self.df_x_future.iloc[[-i]]
            y_pred = self.m.predict(self.df_x_future)

            self.total_series.iloc[-i] = y_pred

        fcst_value = self.total_series.iloc[-period:]

        # re-initialise index so that it is a DatetimeIndex object with the correct 'freq'
        fcst_value.index = fcst_index

        if self.growth:
            fcst_value.name = 'growth'
            fcst_value = gr_to_val(self.series_y, fcst_value, self.periods)

        return fcst_value


class PProphet:
    def __init__(self, series_y):
        self.series_y = series_y
        self.df_y = pd.DataFrame(series_y).reset_index()

    def fit(self):
        self.m = Prophet()
        self.m.fit(self.df_y)
        return self

    def forecast(self, period):
        future = self.m.make_future_dataframe(
            period, freq=self.series_y.index.freq, include_history=False
        )
        prediction = self.m.predict(future)
        _forecast = prediction[['ds', 'yhat']].set_index('ds')['yhat']
        _forecast.name = 'forecast'

        return _forecast


def expo01(series_y):
    if len(series_y) <= 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = SimpleExpSmoothing(series_y).fit()
    return m


def expo02(series_y):
    if len(series_y) <= 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = ExponentialSmoothing(series_y, trend='add').fit()
    return m


def expo03(series_y):
    if len(series_y) < 24:
        m = DefaultModel(series_y)
        return m.fit()
    m = ExponentialSmoothing(
        series_y, trend='add', seasonal='add', seasonal_periods=12
    ).fit()
    return m


def arima01(series_y):
    if len(series_y) < 3:
        m = DefaultModel(series_y)
        return m.fit()

    m = Arima(series_y, growth=False).fit()
    return m


def arima02(series_y):
    series_y_gr = val_to_gr(series_y, periods=12)
    if len(series_y_gr) < 3:
        m = DefaultModel(series_y_gr)
        return m.fit()

    m = Arima(series_y, growth=True).fit()
    return m


# def autoarima01(series_y):
#     if len(series_y) < 5:
#         m = DefaultModel(series_y)
#         return m.fit()
#     m = Arima(series_y, auto=True, growth=False).fit()
#     return m


# def autoarima02(series_y):
#     series_y_gr = val_to_gr(series_y, periods=12)
#     if len(series_y_gr) < 5:
#         m = DefaultModel(series_y_gr)
#         return m.fit()
#     m = Arima(series_y, auto=True, growth=True).fit()
#     return m


def sma01(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(window=3)
    return m


def sma02(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(window=6)
    return m


def sma03(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(window=12)
    return m


def wma01(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(method='weighted', window=3)
    return m


def wma02(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(method='weighted', window=6)
    return m


def wma03(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(method='weighted', window=12)
    return m


def ema01(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(method='exponential', window=3)
    return m


def ema02(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(method='exponential', window=6)
    return m


def ema03(series_y):
    if len(series_y) < 1:
        m = DefaultModel(series_y)
        return m.fit()
    m = MovingAverage(series_y)
    m = m.fit(method='exponential', window=12)
    return m


def randomforest01(series_y):
    if len(series_y) < 25:
        m = DefaultModel(series_y)
        return m.fit()
    m = RandomForest(series_y)
    m = m.fit()
    return m


def randomforest02(series_y):
    series_y_gr = val_to_gr(series_y, periods=12)
    if len(series_y_gr) < 25:
        m = DefaultModel(series_y_gr)
        return m.fit()
    m = RandomForest(series_y, growth=True)
    m = m.fit()
    return m


def prophet01(series_y, freq=None):
    if len(series_y) < 3:
        m = DefaultModel(series_y)
        return m.fit()
    m = PProphet(series_y)
    m = m.fit()
    return m


model_dict = {
    'expo01': expo01, 'expo02': expo02, 'expo03': expo03,
    'sma01': sma01, 'sma02': sma02, 'sma03': sma03,
    'wma01': wma01, 'wma02': wma02, 'wma03': wma03,
    'ema01': ema01, 'ema02': ema02, 'ema03': ema03,
    'arima01': arima01, 'arima02': arima02,
    # 'autoarima01': autoarima01, 'autoarima02': autoarima02,
    'randomforest01': randomforest01, 'randomforest02': randomforest02,
    'prophet01': prophet01
}


def forecast_model(series_y, model, data_date, fcst_period):
    series_y = series_y[series_y.index <= data_date]
    m = model_dict[model](series_y)
    r = m.forecast(fcst_period)
    r.index.name = 'ds'
    r.name = 'forecast'
    return r


def run_model(df_y, model, data_date, fcst_period, test_period):
    series_y = df_y.loc[df_y.index <= data_date, 'y']
    if len(series_y) == 0:
        r = pd.Series(dtype=np.float64)
        r.index.name = 'ds'
        r.name = 'forecast'
        return {'val_results': r, 'val_mae': np.inf, 'forecast_results': r}
    date_data = dt.date.fromisoformat(data_date)

    res = []
    for dif in range(test_period + 1):
        date_test = date_data + relativedelta(months=-dif)
        r = forecast_model(series_y, model, date_test.isoformat(), fcst_period)
        res.append(r)

    # Gather results for the validation periods
    # Results for the forecasting period is 0
    val_list = [
        pd.merge(
            res[i], series_y, how='inner', left_index=True, right_index=True
        )
        for i in range(1, test_period + 1)
    ]
    val_results = pd.concat(val_list)
    if len(val_results) > 0:
        ae = abs(val_results['forecast'] - val_results['y'])
        val_mae = sum(ae) / len(ae)
    else:
        val_mae = np.inf

    forecast_results = res[0]

    return {
        'val_results': val_results,
        'val_mae': val_mae,
        'forecast_results': forecast_results
    }


def run_all_models(
        df_y,
        id_value,
        models,
        data_date,
        fcst_period,
        test_period,
        top_n
):
    res_dict = {
        m: run_model(df_y, m, data_date, fcst_period, test_period)
        for m in models
    }

    # Sort the results based on the validation MAE
    res_dict = sorted(res_dict.items(), key=lambda x: x[1]['val_mae'])
    top_n_models = res_dict[:top_n]

    forecast_results = (
        pd.concat([i[1]['forecast_results'] for i in top_n_models])
            .groupby(level=0)
            .mean()
    )
    top_models = [i[0] for i in top_n_models]
    top_models = '|'.join(top_models)

    forecast_results = forecast_results.to_frame()
    forecast_results['id'] = id_value
    forecast_results['top_models'] = top_models

    return {'val_dict': res_dict, 'forecast_results': forecast_results}


def forecast(df, models, data_date, fcst_start, fcst_end, test_period, top_n):
    """
    Returns
    -------
    DataFrame
        Final forecast results (average of the top models)
    """

    date_data = dt.date.fromisoformat(data_date)
    date_fcst_start = dt.date.fromisoformat(fcst_start)
    date_fcst_end = dt.date.fromisoformat(fcst_end)

    if date_data >= date_fcst_start:
        # detect/convert to start month
        raise FuncRaiseError('Data date must be before forecast start date.')

    # Calculate forecast period
    date_diff = relativedelta(date_fcst_end, date_data)
    fcst_period = date_diff.years * 12 + date_diff.months

    forecast_list = []
    for id_value in df['id'].unique():
        df_test_i = resample(df, id_value, data_date)
        r_dict = run_all_models(
            df_test_i,
            id_value,
            models,
            data_date,
            fcst_period,
            test_period,
            top_n
        )
        forecast_list.append(r_dict['forecast_results'])
    results = pd.concat(forecast_list)

    return results.loc[results.index >= fcst_start]


# module function --------------------------------------------------------------
def run_tbl_forecast(
        input_df: pd.DataFrame,
        run_date: str,
        forecast_top_model: int,
        forecast_test_period: int,
        forecast_end_period: int,
        model: list,
        date_range_sla_month_forecast: int
):
    """
    forecast data
    """
    upload_datetime = get_run_date(date_type='date_time').isoformat()
    if date_range_sla_month_forecast > 0:
        run_date = (
                dt.date.fromisoformat(run_date)
                + relativedelta(months=date_range_sla_month_forecast)
        ).strftime('%Y-%m-%d')
    forecast_start = get_process_date(run_date, 'monthly')
    forecast_end: str = (
            dt.date.fromisoformat(run_date).replace(day=1) +
            relativedelta(months=forecast_end_period)
    ).strftime('%Y-%m-%d')
    data_date: str = str(input_df["ds"].max())

    # works for both str and datetime inputs
    input_df['ds'] = pd.to_datetime(input_df['ds'], format='%Y-%m-%d')
    forecast_results = forecast(
        input_df,
        model,
        data_date,
        forecast_start,
        forecast_end,
        forecast_test_period,
        forecast_top_model,
    )
    forecast_results.loc[forecast_results['forecast'] < 0, 'forecast'] = 0
    values: list = [
        (
            f"('{row['id']}', {row['forecast']}, "
            f"'{i.date().isoformat()}', '{run_date}', "
            f"'{upload_datetime}')"
        )
        for i, row in forecast_results.iterrows()
    ]

    result_values: str = ','.join(values)
    logger.info("Forecast value will update to database")
    return result_values
