import sqlite3


class dataHandler(object):

    @classmethod
    def create_connection(cls):
        connection = sqlite3.connect("sw_data.db")
        cursor = connection.cursor()
        return cursor


    @classmethod
    def clear_table(cls, table_name):
        ok = ""
        try:
            cursor = dataHandler.create_connection()
            sql = f"delete from {table_name}"
            cursor.execute(sql)
            cursor.connection.commit()
            cursor.connection.close()
        except Exception as why:
            ok = str(why)

        return ok

    @classmethod
    def insert_species_record(cls, record_data):
        ok = ""
        try:
            cursor = dataHandler.create_connection()
            sql = f"insert into species ({dataHandler.ret_field_names('species')}) values " \
                  "(?,?,?,?,?,?,?,?,?,?,?,?)"

            fields = []
            fields.append(record_data['id'])
            fields.append(record_data['name'])
            fields.append(record_data['classification'])
            fields.append(record_data['designation'])
            fields.append(record_data['average_height'])
            fields.append(record_data['skin_colors'])
            fields.append(record_data['hair_colors'])
            fields.append(record_data['eye_colors'])
            fields.append(record_data['average_lifespan'])
            fields.append(record_data['homeworld'])
            fields.append(record_data['language'])
            fields.append(record_data['url'])
            cursor.execute(sql, fields)
            cursor.connection.commit()
            cursor.connection.close()
        except Exception as why:
            ok = str(why)

        return ok

    @classmethod
    def insert_person_record(cls, record_data):
        ok = ""
        try:
            cursor = dataHandler.create_connection()
            sql = f"insert into people ({dataHandler.ret_field_names('people')}) values " \
                  "(?,?,?,?,?,?,?,?,?,?,?,?)"

            fields = []
            fields.append(record_data['name'])
            fields.append(record_data['height'])
            fields.append(record_data['mass'])
            fields.append(record_data['hair_color'])
            fields.append(record_data['skin_color'])
            fields.append(record_data['eye_color'])
            fields.append(record_data['birth_year'])
            fields.append(record_data['gender'])
            fields.append(record_data['apperances'])
            fields.append(record_data['species_name'])
            fields.append(record_data['species_url'])
            fields.append(record_data['url'])
            cursor.execute(sql, fields)
            cursor.connection.commit()
            cursor.connection.close()
        except Exception as why:
            ok = str(why)

        return ok

    @classmethod
    def ret_field_names(cls, table_name):
        field_list = ""
        if table_name == "people":
            field_list = "name, height, mass, hair_colour, skin_colour, eye_colour, birth_year, " \
                         "gender, appearances, species_name, species_url, url "
        elif table_name == "species":
            field_list = "ID, name, classification, designation, average_height, skin_colors, hair_colors, " \
                         "eye_colors, average_lifespan, homeworld, language, url "
        return field_list

    @classmethod
    def ret_last_run_date_time(cls):
        ok = "No load in log"
        try:
            cursor = dataHandler.create_connection()
            sql = "select max(date_time) from load_log where log_item = 'finished'"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result[0] is not None:
                ok = result[0]
        except Exception as why:
            ok = '01/01/1900 00:00:00'

        finally:
            cursor.connection.close()

        return ok

    @classmethod
    def is_import_currently_running(cls):
        still_running = 1
        try:
            cursor = dataHandler.create_connection()
            sql = "select log_item from load_log order by date_time desc limit 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result[0] is not None:
                ok = result[0]
        except Exception as why:
            ok = why

        finally:
            cursor.connection.close()

        if ok == "finished":
            still_running = 0

        return still_running


    @classmethod
    def insert_log_item(cls, log_id, log_data):
        ok = ""
        try:
            cursor = dataHandler.create_connection()
            sql = "insert into load_log (work_id, log_item) values " \
                  "(?,?)"
            cursor.execute(sql, (log_id, log_data))
            cursor.connection.commit()
            cursor.connection.close()
        except Exception as why:
            ok = why
        return ok

    @classmethod
    def grab_log_entries(cls, log_id):
        cursor = dataHandler.create_connection()
        sql = "select date_time, log_item from load_log where work_id = ?"
        cursor.execute(sql, (log_id,))
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(dict(zip(columns, row))))

        return results

    @classmethod
    def grab_work_id(cls):
        work_id = 0
        try:
            cursor = dataHandler.create_connection()
            sql = "select max(work_id) from load_log where log_item = 'finished' " \
                  "and work_id in (select max(work_id) from load_log)"

            cursor.execute(sql)
            result = cursor.fetchone()
            if result[0] is not None:
                work_id = abs(result[0]) + 1
        except Exception as why:
            work_id = 0

        finally:
            cursor.connection.close()

        return work_id

    @classmethod
    def ret_top10_people_most_films_height(cls):
        cursor = dataHandler.create_connection()
        sql = "select lower(name) as name, height from ( "\
              "SELECT name,"\
              " height,"\
              " appearances "\
              " FROM people "\
              " order by abs(appearances) desc " \
              " limit 10 ) "\
              " order by abs(height) desc"
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(dict(zip(columns, row))))

        return results

    @classmethod
    def ret_csv_data(cls):
        cursor = dataHandler.create_connection()
        sql = "SELECT name, species_name as species, height, appearances FROM people;"
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(dict(zip(columns, row))))

        return results

    @classmethod
    def ret_species_name(cls, species_id):
        try:
            cursor = dataHandler.create_connection()
            sql = "select name from species where id = ?"
            cursor.execute(sql, (species_id,))
            result = cursor.fetchone()
            species = result[0]
            if result[0] is None:
                species = ""

        except Exception as why:
            print(why)
        return species

