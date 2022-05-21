# -*- coding=utf-8 -*-

import mysql.connector
import sys
import datetime
from settings import DB_SOURCE, DB_TARGET, TABLES_CHECK, DB_CHECKSUM

global tbl_key_end


def get_db_conn(**db_conn_info):
    db_host = db_conn_info['host']
    db_user = db_conn_info['user']
    db_pass = db_conn_info['password']
    db_port = db_conn_info['port']
    db_name = db_conn_info.get('database', None)
    db_charset = db_conn_info.get('charset', 'utf8mb4')

    try:
        conn = mysql.connector.connect(host=db_host,
                                       user=db_user,
                                       password=db_pass,
                                       port=db_port,
                                       charset=db_charset,
                                       use_unicode=True)

    except mysql.connector.Error as e:
        print("Error %s: %s" % (e.args[0], e.args[1]))
        sys.exit(-1)

    return conn


def before_checksum(db, tbl):

    checksum_db_conn = get_db_conn(**DB_CHECKSUM)
    checksum_db_cur = checksum_db_conn.cursor(dictionary=True)

    sql_str = """
        CREATE TABLE IF NOT EXISTS `checksum`.`t_checksum` (
             db CHAR(64) NOT NULL,
             tbl CHAR(64) NOT NULL,
             chunk INT NOT NULL,
             chunk_time FLOAT NULL,
             chunk_index VARCHAR(200) NULL,
             lower_boundary TEXT NULL,
             upper_boundary TEXT NULL,
             chunk_key_start bigint NOT NULL,
             chunk_key_end bigint NOT NULL,
             source_crc CHAR(40) NOT NULL,
             source_cnt INT NOT NULL,
             target_crc CHAR(40) NULL,
             target_cnt INT NULL,
             ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
             PRIMARY KEY (db, tbl, chunk),
             INDEX ts_db_tbl (ts, db, tbl)
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            """
    checksum_db_cur.execute(sql_str)

    sql_str = "DELETE FROM `checksum`.`t_checksum` WHERE db=%s and tbl=%s"
    param = (db, tbl)

    checksum_db_cur.execute(sql_str, param)
    checksum_db_conn.commit()
    checksum_db_cur.close()
    checksum_db_conn.close()


def get_cols(db_name, table_name, **db_info):

    db_conn = get_db_conn(**db_info)
    db_cur = db_conn.cursor(dictionary=True)

    sql_cols_cols = "select GROUP_CONCAT(COLUMN_NAME ORDER BY ordinal_position) as cols " \
                    "from information_schema.COLUMNS " \
                    "where table_schema=%s and table_name =%s"

    sql_param = (db_name, table_name)

    db_cur.execute(sql_cols_cols, sql_param)
    res = db_cur.fetchall()

    db_cur.close()
    db_conn.close()

    if res[0]['cols'] is not None:
        return res[0]['cols']
    else:
        print("Error: table %s does not exist" % table_name)
        sys.exit(-1)


def get_pri_key(db_name, table_name, **db_info):

    db_conn = get_db_conn(**db_info)
    db_cur = db_conn.cursor(dictionary=True)

    sql_pri_key = "select COLUMN_NAME as pri_key from information_schema.`COLUMNS` " \
                  "where table_schema=%s and table_name=%s and column_key='PRI'"

    sql_param = (db_name, table_name)

    db_cur.execute(sql_pri_key, sql_param)
    res = db_cur.fetchall()

    db_cur.close()
    db_conn.close()

    if res[0]['pri_key'] is not None:
        return res[0]['pri_key']
    else:
        print("Error: table %s does not exist" % table_name)
        sys.exit(-1)


def source_checksum(db, tbl):

    before_checksum(db, tbl)

    tab_cols = get_cols(db, tbl, **DB_SOURCE)
    pri_key = get_pri_key(db, tbl, **DB_SOURCE)

    source_db_conn = get_db_conn(**DB_SOURCE)
    source_db_cur = source_db_conn.cursor(dictionary=True)

    checksum_db_conn = get_db_conn(**DB_CHECKSUM)
    checksum_db_cur = checksum_db_conn.cursor(dictionary=True)

    chunk_no = 0

    while True:

        chunk_key_start, chunk_key_end = get_chunk_range(db, tbl, pri_key, chunk_no, checksum_db_cur)

        if chunk_key_start != chunk_key_end:
            sql_str = "SELECT " \
                      "COUNT(*) AS cnt, " \
                      "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc " \
                      "FROM %s.%s FORCE INDEX (`PRIMARY`) " \
                      "WHERE (%s >= %s AND %s < %s)" \
                      % (tab_cols, db, tbl, pri_key, chunk_key_start, pri_key, chunk_key_end)
        else:
            sql_str = "SELECT " \
                      "COUNT(*) AS cnt, " \
                      "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc " \
                      "FROM %s.%s FORCE INDEX (`PRIMARY`) " \
                      "WHERE (%s = %s)" % (tab_cols, db, tbl, pri_key, chunk_key_end)

        source_db_cur.execute(sql_str)
        res = source_db_cur.fetchall()

        row_cnt = res[0]['cnt']
        chunk_crc = res[0]['crc']

        res_chunk_sql = "insert into `checksum`.`t_checksum` " \
                        "(db, tbl, chunk, chunk_key_start, chunk_key_end, source_cnt, source_crc)" \
                        "values (%s, %s, %s, %s, %s, %s, %s)"
        res_chunk_para = (db, tbl, chunk_no, chunk_key_start, chunk_key_end, row_cnt, chunk_crc)
        # print(res_chunk_para)
        checksum_db_cur.execute(res_chunk_sql, res_chunk_para)
        checksum_db_conn.commit()

        chunk_no += 1

        if chunk_key_start == chunk_key_end:
            source_db_cur.close()
            source_db_conn.close()
            checksum_db_cur.close()
            checksum_db_conn.close()
            break
    return "Source table: %s.%s checksum done." % (db, tbl)


def target_checksum(db, tbl):

    tab_cols = get_cols(db, tbl, **DB_TARGET)
    pri_key = get_pri_key(db, tbl, **DB_TARGET)

    target_db_conn = get_db_conn(**DB_TARGET)
    target_db_cur = target_db_conn.cursor(dictionary=True)

    checksum_db_conn = get_db_conn(**DB_CHECKSUM)
    checksum_db_cur = checksum_db_conn.cursor(dictionary=True)

    sql_str = "select chunk, chunk_key_start, chunk_key_end " \
              "from `checksum`.`t_checksum` FORCE INDEX (PRIMARY)" \
              "where db='%s' and tbl='%s'" % (db, tbl)
    checksum_db_cur.execute(sql_str)
    res = checksum_db_cur.fetchall()

    for chunk in res:
        chunk_key_start, chunk_key_end = (chunk['chunk_key_start'], chunk['chunk_key_end'])

        if chunk_key_start != chunk_key_end:
            sql_str = "SELECT " \
                      "COUNT(*) AS cnt, " \
                      "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc " \
                      "FROM %s.%s FORCE INDEX (`PRIMARY`) " \
                      "WHERE (%s >= %s AND %s < %s)" \
                      % (tab_cols, db, tbl, pri_key, chunk_key_start, pri_key, chunk_key_end)
        else:
            sql_str = "SELECT " \
                      "COUNT(*) AS cnt, " \
                      "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc " \
                      "FROM %s.%s FORCE INDEX (`PRIMARY`) " \
                      "WHERE (%s = %s)" % (tab_cols, db, tbl, pri_key, chunk_key_end)

        target_db_cur.execute(sql_str)
        res = target_db_cur.fetchall()

        row_cnt = res[0]['cnt']
        chunk_crc = res[0]['crc']

        res_chunk_sql = "update `checksum`.`t_checksum` FORCE INDEX (PRIMARY)" \
                        "set `target_cnt`=%s, `target_crc`=%s " \
                        "where `chunk`=%s and `db`=%s and `tbl`=%s"
        res_chunk_para = (row_cnt, chunk_crc, chunk['chunk'], db, tbl)
        # print(res_chunk_para)
        checksum_db_cur.execute(res_chunk_sql, res_chunk_para)
        checksum_db_conn.commit()

    target_db_cur.close()
    target_db_conn.close()
    checksum_db_cur.close()
    checksum_db_conn.close()
    return "Target table: %s.%s checksum done." % (db, tbl)


def get_chunk_range(db_name, table_name, primary_key, chunk_no, cs_db_cur):
    chunk = 1500
    source_db_conn = get_db_conn(**DB_SOURCE)
    source_db_cur = source_db_conn.cursor(dictionary=True)

    if chunk_no == 0:
        sql_str = "SELECT MIN(%s) as min_value, MAX(%s) as max_value FROM %s.%s FORCE INDEX (PRIMARY)" \
                  % (primary_key, primary_key, db_name, table_name)

        source_db_cur.execute(sql_str)
        res = source_db_cur.fetchall()

        tbl_key_start = res[0]['min_value']
        globals()['tbl_key_end'] = res[0]['max_value']
        chunk_key_start = tbl_key_start

        source_db_cur.close()
        source_db_conn.close()

    else:
        sql_str = "SELECT MAX(`chunk_key_end`) as chunk_key_end FROM `checksum`.`t_checksum` FORCE INDEX (PRIMARY)" \
                  "WHERE db='%s' and tbl='%s'" % (db_name, table_name)
        # print(sql_str)
        cs_db_cur.execute(sql_str)
        res = cs_db_cur.fetchall()
        # print(res[0])
        chunk_key_start = res[0]['chunk_key_end']

    if chunk_key_start + chunk <= tbl_key_end:
        return chunk_key_start, chunk_key_start + chunk
    else:
        return chunk_key_start, tbl_key_end


def chunk_compare(db, tbl):
    cs_db_conn = get_db_conn(**DB_CHECKSUM)
    cs_db_cur = cs_db_conn.cursor(dictionary=True)

    compare_sql = "SELECT `chunk`, `chunk_key_start`, `chunk_key_end` " \
                  "FROM `checksum`.`t_checksum` FORCE INDEX (PRIMARY) " \
                  "WHERE `db`=%s AND `tbl`=%s " \
                  "AND (`source_cnt`<>`target_cnt` OR `source_crc`<>`target_crc`)"
    compare_para = (db, tbl)

    cs_db_cur.execute(compare_sql, compare_para)
    res = cs_db_cur.fetchall()

    if cs_db_cur.rowcount != 0:
        return res
    else:
        return False


def checksum_rows(db, tbl, key_start, key_end, **db_info):

    db_conn = get_db_conn(**db_info)
    db_cur = db_conn.cursor(dictionary=True)

    tab_cols = get_cols(db, tbl, **db_info)
    pri_key = get_pri_key(db, tbl, **db_info)

    sql_str = "select %s as pri_key " \
              "from %s.%s FORCE INDEX (`PRIMARY`) " \
              "where %s >= %s and %s < %s" % (pri_key, db, tbl, pri_key, key_start, pri_key, key_end)

    db_cur.execute(sql_str)
    chunk_pri_key_list = db_cur.fetchall()

    row_crc_list = {}

    for per_pri_key in chunk_pri_key_list:
        sql_str = "SELECT " \
                  "COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#', %s)) AS UNSIGNED)), 10, 16)), 0) AS crc " \
                  "FROM %s.%s FORCE INDEX (`PRIMARY`) " \
                  "WHERE (%s = %s)" % (tab_cols, db, tbl, pri_key, per_pri_key['pri_key'])
        # print(sql_str)
        db_cur.execute(sql_str)
        per_row_checksum = db_cur.fetchall()

        row_crc_list[per_pri_key['pri_key']] = per_row_checksum[0]['crc']

    return row_crc_list


def isnone(value):
    if value is None:
        return "NULL"
    else:
        return value


def general_fix_sql(sql_type, db, tbl, key_value, **db_info):
    db_conn = get_db_conn(**db_info)
    db_cur = db_conn.cursor(dictionary=True)

    pri_key = get_pri_key(db, tbl, **db_info)

    if sql_type == 'replace':

        sql_str = "SELECT * FROM %s.%s FORCE INDEX (`PRIMARY`) WHERE %s=%s" % (db, tbl, pri_key, key_value)
        db_cur.execute(sql_str)
        res = db_cur.fetchall()

        row_key = ', '.join('`' + str(v) + '`' for v in res[0].keys())
        row_value = ', '.join('\'' + str(v) + '\'' if isinstance(v, str) or isinstance(v, datetime.datetime) else str(isnone(v)) for v in res[0].values())

        fix_sql = "replace into %s.%s (%s) values (%s);" % (db, tbl, row_key, row_value)

    elif sql_type == 'delete':

        fix_sql = "delete from %s.%s where %s=%s;" % (db, tbl, pri_key, key_value)

    return fix_sql


if __name__ == '__main__':

    for st_name in TABLES_CHECK.split(','):
        schema, table = st_name.split('.')
        print(source_checksum(schema, table))
        print(target_checksum(schema, table))

        compare_res = chunk_compare(schema, table)
        if compare_res:
            for compare_res_row in compare_res:
                dict_source_key_crc = \
                    checksum_rows(schema, table, compare_res_row['chunk_key_start'], compare_res_row['chunk_key_end'], **DB_SOURCE)
                dict_target_key_crc = \
                    checksum_rows(schema, table, compare_res_row['chunk_key_start'], compare_res_row['chunk_key_end'], **DB_TARGET)

                # a集合独有的&a,b集合都有但是值不同的元素，直接replace即可
                set_ins_upd = set(dict_source_key_crc.items()) - set(dict_target_key_crc.items())
                if set_ins_upd:
                    for row in set_ins_upd:
                        print(general_fix_sql('replace', schema, table, row[0], **DB_SOURCE))

                # b集合独有的key，直接删除即可
                set_del = set(dict_target_key_crc.keys()) - set(dict_source_key_crc.keys())
                if set_del:
                    for row in set_del:
                        print(general_fix_sql('delete', schema, table, row, **DB_TARGET))
