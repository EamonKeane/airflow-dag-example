### Need to add this at the top of the file to be able to save images
## These two lines added to resolve https://stackoverflow.com/questions/37604289/tkinter-tclerror-no-display-name-and-no-display-environment-variable?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
import matplotlib
matplotlib.use('Agg')
###
from airflow.hooks.oracle_hook import OracleHook
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

clients = {34: [283], 
            37: [1482, 1543, 284, 1602], 
            57: [285], 
            58: [1442, 1542, 281], 
            78: [781], 84: [261], 
            134: [121, 201, 1], 
            137: [95], 
            152: [100], 
            157: [85, 1182, 961, 901, 601, 1642, 1522, 1483, 521, 481, 1302],
            160: [6], 161: [1722, 103],
            166: [1181, 381, 41], 168: [44], 
            173: [79], 177: [21], 187: [84], 
            188: [1462, 1282, 12, 221, 581, 441, 641, 761, 881, 821, 1242, 321, 1382, 541, 922, 382, 141, 181, 1001, 1502, 1702, 161, 921, 1742, 801, 1762, 501, 1562, 1682, 861, 621, 361, 341, 1422, 561, 1362, 1221, 1645, 241, 1383, 661, 1402, 721, 1582, 1322, 1342, 1622, 1241, 841, 1201, 301, 681, 722, 1523], 
            195: [25], 206: [941, 81], 
            1200: [701, 1664, 462, 1643, 723, 981, 401, 1263, 262], 
            1201: [741, 1663, 1262, 263, 461, 402], 
            1202: [742, 1264, 1665, 421, 463, 264, 1644]}


class SQLTemplatedPythonOperator(PythonOperator):

    # somehow ('.sql',) doesn't work but tuple of two works...
    template_ext = ('.sql', '.abcdefg')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 2),
    'email': ['eamon@logistio.ie'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id="lifetime-template-sql",
    default_args=default_args,
    schedule_interval="@once",
    template_searchpath='/usr/local/airflow/dags/sql'
)


def get_clv(oracle_conn_id, src_client_id, storage_bucket, ds, **context):
    import matplotlib.pyplot
    matplotlib.pyplot.ioff()
    ##
    from lifetimes.utils import calibration_and_holdout_data
    from lifetimes.plotting import plot_frequency_recency_matrix
    from lifetimes.plotting import plot_probability_alive_matrix
    from lifetimes.plotting import plot_calibration_purchases_vs_holdout_purchases
    from lifetimes.plotting import plot_period_transactions
    from lifetimes.plotting import plot_history_alive
    from lifetimes.plotting import plot_cumulative_transactions
    from lifetimes.utils import expected_cumulative_transactions
    from lifetimes.utils import summary_data_from_transaction_data
    from lifetimes import BetaGeoFitter
    from lifetimes import GammaGammaFitter
    import datetime
    import pandas as pd
    import datalab.storage as gcs
    conn = OracleHook(oracle_conn_id=oracle_conn_id).get_conn()
    print(src_client_id, context)
    query = context['templates_dict']['query']
    data = pd.read_sql(query, con=conn)
    data.columns = data.columns.str.lower()
    print(data.head())

    # Calculate RFM values#
    calibration_end_date = datetime.datetime(2018, 5, 24)
    training_rfm = calibration_and_holdout_data(transactions=data,
                                                customer_id_col='src_user_id',
                                                datetime_col='pickup_date',
                                                calibration_period_end=calibration_end_date,
                                                freq='D',
                                                monetary_value_col='price_total')
    bgf = BetaGeoFitter(penalizer_coef=0.0)
    bgf.fit(training_rfm['frequency_cal'],
            training_rfm['recency_cal'], training_rfm['T_cal'])
    print(bgf)

    # Matrix charts
    plot_period_transactions_chart = context.get("ds_nodash") + str(src_client_id)+'_plot_period_transactions_chart.svg'
    plot_frequency_recency_chart = context.get("ds_nodash") + str(src_client_id)+'_plot_frequency_recency_matrix.svg'
    plot_probability_chart = context.get("ds_nodash") + str(src_client_id)+'_plot_probability_alive_matrix.svg'
    plot_calibration_vs_holdout_chart = context.get("ds_nodash") + str(src_client_id)+'_plot_calibration_vs_holdout_purchases.svg'

    ax0 =  plot_period_transactions(bgf, max_frequency=30)
    ax0.figure.savefig(plot_period_transactions_chart, format='svg')
    ax1 =  plot_frequency_recency_matrix(bgf)
    ax1.figure.savefig(plot_frequency_recency_chart, format='svg')
    ax2 = plot_probability_alive_matrix(bgf)
    ax2.figure.savefig(plot_probability_chart, format='svg')
    ax3 = plot_calibration_purchases_vs_holdout_purchases(bgf, training_rfm, n=50)
    ax3.figure.savefig(plot_calibration_vs_holdout_chart, format='svg')
    full_rfm = summary_data_from_transaction_data(data, 
                                              customer_id_col='src_user_id', 
                                              datetime_col='pickup_date',
                                              monetary_value_col='price_total', 
                                              datetime_format=None,
                                              observation_period_end=None,
                                              freq='D')
    returning_full_rfm = full_rfm[full_rfm['frequency'] > 0]
    ggf = GammaGammaFitter(penalizer_coef = 0)
    ggf.fit(returning_full_rfm['frequency'],
    returning_full_rfm['monetary_value'])

    customer_lifetime=30 # expected number of months lifetime of a customer
    clv = ggf.customer_lifetime_value(
        bgf, #the model to use to predict the number of future transactions
        full_rfm['frequency'],
        full_rfm['recency'],
        full_rfm['T'],
        full_rfm['monetary_value'],
        time=customer_lifetime, # months
        discount_rate=0.01 # monthly discount rate ~ 12.7% annually
        ).sort_values(ascending=False)
    full_rfm_with_value = full_rfm.join(clv)

    full_rfm_file = context.get("ds_nodash") + "-src_client_id-"+ str(src_client_id) + '-icabbi-test.csv' 
    full_rfm_with_value.to_csv(full_rfm_file)
    GoogleCloudStorageHook(google_cloud_storage_conn_id='google_conn_default').upload(bucket=storage_bucket,
                                                                                      object=str(src_client_id) + "/" + context.get("ds_nodash") + "/" + full_rfm_file,
                                                                                      filename=full_rfm_file)
    GoogleCloudStorageHook(google_cloud_storage_conn_id='google_conn_default').upload(bucket=storage_bucket,
                                                                                      object=str(src_client_id) + "/" + context.get("ds_nodash") + "/" + plot_period_transactions_chart,
                                                                                      filename=full_rfm_file)
    GoogleCloudStorageHook(google_cloud_storage_conn_id='google_conn_default').upload(bucket=storage_bucket,
                                                                                    object=str(src_client_id) + "/" + context.get("ds_nodash") + "/" + plot_frequency_recency_chart,
                                                                                    filename=full_rfm_file)
    GoogleCloudStorageHook(google_cloud_storage_conn_id='google_conn_default').upload(bucket=storage_bucket,
                                                                                      object=str(src_client_id) + "/" + context.get("ds_nodash") + "/" + plot_probability_chart,
                                                                                      filename=full_rfm_file)
    GoogleCloudStorageHook(google_cloud_storage_conn_id='google_conn_default').upload(bucket=storage_bucket,
                                                                                      object=str(src_client_id) + "/" + context.get("ds_nodash") + "/" + plot_calibration_vs_holdout_chart,
                                                                                      filename=full_rfm_file)


def get_clients(oracle_conn_id, ds, **context):
    import pandas as pd
    query = """SELECT CLIENT_ID, SRC_CLIENT_ID
                FROM DWH.WC_CLIENT_D
                GROUP BY SRC_CLIENT_ID, CLIENT_ID"""
    conn = OracleHook(oracle_conn_id=oracle_conn_id).get_conn()
    data = pd.read_sql(query, con=conn)
    data.columns = data.columns.str.lower()
    # get dictionary of values e.g. "{34: [283], 37: [1482, 284, 1602, 1543],57: [285]}"
    data = data.groupby('src_client_id')['client_id'].apply(
        lambda data: data.tolist()).to_dict()
    client_list = data
    return client_list


with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')
    for src_client_id, dwh_client_list in clients.items():
        # To produce a list of strings for inserting into where statement in sql e.g.:
        #   34 (283)
        #   37 (1482, 1543, 284, 1602)
        dwh_client_ids = str(tuple(dwh_client_list)).rstrip(',)') + ')'
        calculate_bgf = SQLTemplatedPythonOperator(
        templates_dict={'query': 'bookings_clv.sql'},
        op_kwargs={"oracle_conn_id": "oracle_icabbi", 
                    "src_client_id": src_client_id,
                    "storage_bucket": "icabbi-data"},
        task_id='get_clv-'+ str(src_client_id),
        params={"dwh_client_ids": dwh_client_ids, 
                "bookings_start_date": "2018/05/23",
                "bookings_end_date": "2018/05/26"},
        python_callable=get_clv,
        provide_context=True
        )
        kick_off_dag >> calculate_bgf


