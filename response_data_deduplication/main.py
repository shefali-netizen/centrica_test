from datetime import datetime, timedelta
from query_tools import run_query, get_query_status, wait_query_execution, query_athena, retrieve_query_results
from queries import dup_ranked_in_day, query_prev_dups, query_duplicate_table
from config import EXECUTION_DATE


def str_fill(value: int, string_length: int = 2):
    """fill string with zero such that its str(string).__len__() == n"""
    return str(value).zfill(string_length)


# def get_partition_values(days_back: int, level: str = 'hour') -> list:
#     """get the partition (directory names)
#     for today's date.
#
#     ex.: if date today is 28/07/2021 and the time is 12:00 pm (UTC), then this will return:
#     [
#         year='2021', month='07', day='28', hour='0'
#         year='2021', month='07', day='28', hour='1'
#         year='2021', month='07', day='28', hour='2'
#         year='...
#         year='2021', month='07', day='28', hour='12'
#     ]"""
#     now = datetime.utcnow() - timedelta(days=days_back)
#     year = now.year
#     month = now.month
#     day = now.day
#     hour = 23
#
#     if level == 'hour':
#         output = list()
#         for it in range(hour + 1):
#             if it < 24:  # avoiding anything 24 or greater
#                 output.append([str(year), str_fill(month), str_fill(day), str_fill(it)])
#     else:
#         output = [str(year), str_fill(month), str_fill(day)]
#     return output
#
#
# def run_alter_tables(date: datetime = None, ndays: int = 1):
#     """Run ALTER to tables that require incorporating new partitions"""
#     if date is None:
#         # Generate partitions as per ndays backfilling
#         partition_values = get_partition_values(days_back=ndays)
#
#     else:
#         # Generate partitions for the date specified
#         partition_values = []
#         for it in range(24):
#             partition_values.append([str(date.year), str_fill(date.month), str_fill(date.day), str_fill(it)])
#
#     for table in ALTER_TABLES:
#         query = alter_table_query(partition_values, database=table['database'], table_name=table['table_name'],
#                                   location=table['location'], version=table['version'])
#         query_id = run_query(query)
#         status = wait_query_execution(query_id)
#         if status['state'] == 'FAILED':
#             if status['reason'] != PARTITION_EXISTS:
#                 raise Exception(status['reason'])
#             print(f'Status: {status["state"]} \tquery ID: {query_id} '
#                   f'\ttable: {table["table_name"]} \treason: {status["reason"]}')
#         else:
#             print(f'Partitions added to the table: {partition_values}')
#
#
# def data_found(db: str, table: str, year: str, month: str, day: str) -> bool:
#     """
#     Check if data exists for the period and source specified
#     :param exec_date: datetime
#     :param source:    source (filter)
#     :return:          True if data is found
#     """
#     response = query_athena(query_check_data(db, table, year, month, day))
#     if int(response['counter'].tolist()[0]) > 0:
#         return True
#     return False


def run_deduplication(date: datetime = None):
    """Run deduplication in response data"""
    if date is None:
        raise ValueError('A date must be provided')

    year = str(date.year)
    month = str_fill(date.month)
    day = str_fill(date.day)

    print(f'Retrieving duplicates for: {date}')

    query_str_duplicates_in_day = dup_ranked_in_day(year, month, day)
    query_id_dups_in_day = run_query(query_str_duplicates_in_day)
    status = wait_query_execution(query_id_dups_in_day)
    if status['state'] == 'FAILED':
        raise Exception(status['reason'])
    df_dups_in_day = retrieve_query_results(query_id_dups_in_day)


    query_str_prev_duplicates = query_prev_dups(year, month, day)

    query_duplicate_table


def main():
    if EXECUTION_DATE is None:
        raise ValueError('Execution date must be set in the environment')
    exec_date = datetime.strptime(EXECUTION_DATE[:10], '%Y-%m-%d')
    run_deduplication(exec_date)

    return 0


if __name__ == '__main__':
    main()
