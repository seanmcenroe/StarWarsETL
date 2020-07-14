from urllib import request
import json
import time
from data_handler import dataHandler


class SwData(object):

    base_url = "https://swapi.dev/api/"

    @classmethod
    def grab_data(cls, url):
        time.sleep(1)
        resp = request.urlopen(url)
        data = resp.read().decode("utf-8")
        print(f"sent{url} got back {data[0:30]}")
        return data

    @classmethod
    def populate_species_data(cls, work_id):
        ok = "downloaded species data"
        try:
            d = dataHandler
            url = f"{SwData.base_url}species/"
            nomore = False
            while nomore == False:
                r = SwData.grab_data(url)
                dict_data = json.loads(r)
                species_data = dict_data["results"]
                for s in species_data:
                    # append the id to the record
                    tmp = s["url"].split("/")
                    id = tmp[len(tmp) - 2]
                    s.update({'id': id})
                    rec_insert_outcome = d.insert_species_record(s)
                    if len(rec_insert_outcome) > 0:
                        d.insert_log_item(work_id, rec_insert_outcome)

                if dict_data["next"] is not None:
                    url = dict_data["next"]
                else:
                    nomore = True

        except Exception as why:
            ok = f"downloading species data error {why}"

        return ok

    @classmethod
    def grab_people_data(cls, work_id):
        '''
        Used to grab all the people data using the paging to get all results
        there are some calculated fields e.g. appearances which is a count of the films array
        Safety_catch is used to ensure if the process does run away or next is missing then it will
        run 100 times then stop and not hit the server continuously
        :return:
        list of people json readable format
        '''
        url = f"{SwData.base_url}people/"
        d = dataHandler
        d.insert_log_item(work_id, "downloading people data start")
        r = SwData.grab_data(url)
        dict_data = json.loads(r)
        d.insert_log_item(work_id, "downloading people data complete")
        people_data = dict_data["results"]
        nomore = False
        safety_catch = 0
        while nomore == False:
            if dict_data["next"] is not None:
                url = dict_data["next"]
                r = SwData.grab_data(url)
                dict_data = json.loads(r)
                people_data = people_data + dict_data["results"]
            else:
                nomore = True
            safety_catch += 1
            if safety_catch > 100:
                nomore = True
        # clean up the people data ready for storage
        # films are not needed in detail so will only count the number of films
        if len(people_data) > 0:
            d.insert_log_item(work_id, "processed people data start")
            for r in people_data:
                num_films = len(r["films"])
                if r["height"] == "unknown":
                    r["height"] = 0
                r.update({'apperances': num_films})
                species = ""
                tmp = ""
                if len(r["species"]) > 0:
                    tmp = r["species"][0]
                    species_array = tmp.split('/')
                    species_id = species_array[len(species_array)-2]
                    if cls.is_a_number(species_id) == True:
                        print(species_id)
                        species = d.ret_species_name(species_id)

                r.update({'species_name': species})
                r.update({'species_url': tmp})

            d.insert_log_item(work_id, "processed people data complete")
        return people_data


    @classmethod
    def populate_tables(cls, work_id):
        audit = []
        function_out_come = ""
        d = dataHandler
        d.insert_log_item(work_id, "Starting")
        try:
            d.clear_table("species")
            d.insert_log_item(work_id,"clearing down species data")

        except Exception as e:
            d.insert_log_item(work_id, f"clearing down species data error {e}")
            d.insert_log_item(work_id, "finished")

        function_out_come = cls.populate_species_data(work_id)
        d.insert_log_item(work_id, function_out_come)

        people = cls.grab_people_data(work_id)

        d.clear_table("people")

        for r in people:
            ok = d.insert_person_record(r)
            if len(ok) > 0:
                d.insert_log_item(work_id, f"Error inserting character rec  {ok}")

        d.insert_log_item(work_id, "finished")

    @classmethod
    def is_a_number(cls, any_value):
        try:
            val = float(any_value)
            is_number = True
        except ValueError:
            is_number = False

        return is_number