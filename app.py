from flask import Flask, redirect, render_template, request, session, url_for, make_response
from data_handler import dataHandler
from starwars import SwData
import csv
import requests
from io import StringIO
import time

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('Tatooine.html')


@app.route('/reporting/')
@app.route('/reporting/<chartype>')
def reporting(chartype='bar'):

    chart = chartype
    if chartype not in {'bar', 'line'}:
        chart = 'bar'
    d = dataHandler
    characters_by_films_then_height = d.ret_top10_people_most_films_height()
    title = 'Top 10 characters in the most films ordered by height'
    labels = []
    values = []
    for r in characters_by_films_then_height:
        labels.append(r["name"])
        values.append(r["height"])

    return render_template('reporting.html', repvalues=values, labels=labels, title=title, chart=chart)


@app.route('/dataloader')
def dataloader():
    last_run_time = dataHandler.ret_last_run_date_time()
    audit = [{"date_time":"", "log_item": "Logging audit will appear here"}]

    return render_template('dataloader.html', last_run=last_run_time, auditlog=audit)


@app.route('/load')
def load_data():
    audit = []
    import_running = dataHandler.is_import_currently_running()
    if import_running == 0:
        work_id = dataHandler.grab_work_id()
        SwData.populate_tables(work_id)
        audit = dataHandler.grab_log_entries(work_id)
        last_run_time = dataHandler.ret_last_run_date_time()
    else:
        last_run_time = "currently still running"

    return render_template('dataloader.html', last_run=last_run_time, auditlog=audit)


@app.route('/csvexport/')
@app.route('/csvexport/<action>')
def csvexport(action=0):
    '''
    Posting file to httpbin a file is created
    Sending file to client a stream is used
    :param action:
    1 = to post the file to httpbin.org
    2 = to output file to the client
    :return:
    '''
    #Action 1 is send CSV to httpbin
    csv_data = dataHandler.ret_csv_data()
    if action == "1":
        url = "http://httpbin.org/post"
        with open('character.csv', mode='w') as character_file:
            character_writer = csv.writer(character_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            character_writer.writerow(csv_data[0].keys())
            for i, r in enumerate(csv_data):
                character_writer.writerow(csv_data[i].values())
        response = requests.post(url, files={'character.csv': open('character.csv', 'rb')})
        status = response.status_code
        resp = response.text
        return render_template('csvexporter.html', httpbin_status=status, httpbin_response=resp)

    elif action == "2":
        si = StringIO()
        cw = csv.writer(si, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
        cw.writerow(csv_data[0].keys())
        for i, r in enumerate(csv_data):
            cw.writerow(csv_data[i].values())

        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=export.csv"
        output.headers["Content-type"] = "text/csv"
        return output
    else:
        return render_template('csvexporter.html', httpbin_status="", httpbin_response="")


if __name__ == '__main__':
    app.config.update(
        DEBUG=True,
        TESTING=True,
        TEMPLATES_AUTO_RELOAD=True
    )
    app.run(debug=True, templates_auto_reload=True)
