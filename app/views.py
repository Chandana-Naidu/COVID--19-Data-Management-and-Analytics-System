from django.contrib import admin
from django.urls import path, include
from django.contrib import messages
from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect, render

import urllib.request, urllib.error
import sqlite3
import json
import csv
import psycopg2

from covid19tracker import settings
from .data import data


vis_data = ""
database_settings = settings.DATABASES["default"]

database_name = database_settings["NAME"]
database_user = database_settings["USER"]
database_password = database_settings["PASSWORD"]
database_host = database_settings["HOST"]
database_port = database_settings["PORT"]

conn = psycopg2.connect(
    database=database_name,
    user=database_user,
    host=database_host,
    port=database_port,
    password=database_password,
)


def fetch(request):
    conn = psycopg2.connect(
        database=database_name,
        user=database_user,
        host=database_host,
        port=database_port,
        password=database_password,
    )
    cur = conn.cursor()
    try:
        sql_statements = """
        DROP TABLE IF EXISTS States;
        DROP TABLE IF EXISTS Districts;
        DROP TABLE IF EXISTS Cases;
                        
        CREATE TABLE IF NOT EXISTS States (
                    "id" SERIAL PRIMARY KEY,
                    "state" VARCHAR UNIQUE
        );

        CREATE TABLE IF NOT EXISTS Districts (
                    "id" SERIAL PRIMARY KEY,
                    "district" VARCHAR UNIQUE
        );

        CREATE TABLE IF NOT EXISTS Cases
                    ("id" SERIAL PRIMARY KEY, 
                    "state_id" INTEGER, 
                    "district_id" INTEGER,
                    "zone_id" INTEGER,
                    "confirmed" INTEGER, 
                    "active" INTEGER, 
                    "recovered" INTEGER, 
                    "deceased" INTEGER);
        """

        print("Executing SQL statements:")
        # print(sql_statements)
        cur.execute(sql_statements)
    except Exception as e:
        print("exception", e)
    """
    #use data downloaded file from https://api.covid19india.org/ and saved as district_wise.json     
    fname = 'district_wise.json'
    fh = open(fname)
    data = fh.read()
    """
    # from online
    url = "https://api.covid19india.org/state_district_wise.json"
    # print('Retrieving', url)
    info = urllib.request.urlopen(url)
    data = info.read().decode()

    # read data
    js = json.loads(data)
    # print(js)

    # country
    # country_count_confirmed = 0
    # country_count_active = 0
    # country_count_recovered = 0
    # country_count_deceased = 0

    # states
    for state in js:
        # print(state)
        state_count_confirmed = 0
        state_count_active = 0
        state_count_recovered = 0
        state_count_deceased = 0
        cur.execute("SELECT * from States")
        cur.execute(
            "INSERT INTO States(state) VALUES(%s) ON CONFLICT (state) DO NOTHING",
            (state,),
        )
        cur.execute("SELECT id FROM States WHERE state = %s", (state,))
        state_id = cur.fetchone()[0]

        # districts
        for district in js[state]["districtData"]:
            # print(district+":")

            cur.execute(
                "INSERT INTO Districts(district) VALUES(%s) ON CONFLICT (district) DO NOTHING",
                (district,),
            )
            cur.execute("SELECT id FROM Districts WHERE district = %s", (district,))
            district_id = cur.fetchone()[0]

            # confirmed
            confirmed = js[state]["districtData"][district]["confirmed"]
            # print("Confirmed:", confirmed)
            state_count_confirmed += int(confirmed)

            # recovered
            recovered = js[state]["districtData"][district]["recovered"]
            # print("Recovered:", recovered)
            state_count_recovered += int(recovered)

            # deceased
            deceased = js[state]["districtData"][district]["deceased"]
            # print("Deceased:", deceased)
            state_count_deceased += int(deceased)

            # active
            active = confirmed - (recovered + deceased)
            # print("Active:", active)
            state_count_active += active

            cur.execute(
                """INSERT INTO Cases(state_id, district_id, confirmed, active, recovered, deceased) 
                        VALUES(%s, %s, %s, %s, %s, %s) """,
                (state_id, district_id, confirmed, active, recovered, deceased),
            )

        # print("Total confirmed :",state_count_confirmed)
        # print("Total active :",state_count_active)
        # print("Total recovered :",state_count_recovered)
        # print("Total deceased :",state_count_deceased)
        # print("\n")
        # country_count_confirmed += state_count_confirmed
        # country_count_active += state_count_active
        # country_count_recovered += state_count_recovered
        # country_count_deceased += state_count_deceased

    # print(country_count_confirmed,country_count_active, country_count_recovered, country_count_deceased)

    # print("Data Retrieved.")
    cur.execute(
        """
    DROP TABLE IF EXISTS Zones;
                    
    CREATE TABLE IF NOT EXISTS Zones (
                "id" SERIAL PRIMARY KEY,
                "zone" VARCHAR UNIQUE
                )
    """
    )

    """
    #use data downloaded file from https://api.covid19india.org/ and saved as district_wise.json     
    fname = 'zones.json'
    fh = open(fname)
    data = fh.read()
    """

    # from online
    url = "https://api.covid19india.org/zones.json"
    # print('Retrieving', url)
    info = urllib.request.urlopen(url)
    data = info.read().decode()

    # read data
    js = json.loads(data)
    # print(js)

    for value in js["zones"]:
        state = value["state"]
        cur.execute(
            "INSERT INTO States(state) VALUES(%s) ON CONFLICT (state) DO NOTHING",
            (state,),
        )
        cur.execute("SELECT id FROM States WHERE state = %s", (state,))
        state_id = cur.fetchone()[0]

        district = value["district"]
        cur.execute(
            "INSERT INTO Districts(district) VALUES(%s) ON CONFLICT (district) DO NOTHING",
            (district,),
        )
        cur.execute("SELECT id FROM Districts WHERE district = %s", (district,))
        district_id = cur.fetchone()[0]

        cur.execute(
            "SELECT id FROM Cases WHERE state_id = %s AND district_id = %s",
            (state_id, district_id),
        )
        x = cur.fetchone()

        if x is None:
            cur.execute(
                """INSERT INTO Cases(state_id, district_id, confirmed, active, recovered, deceased)
                        VALUES(%s, %s, 0, 0, 0, 0) """,
                (state_id, district_id),
            )

    conn.commit()

    for value in js["zones"]:
        # print("State:", value["state"])
        state = value["state"]
        cur.execute("SELECT id FROM States WHERE state = %s", (state,))
        state_id = cur.fetchone()[0]

        # print("District:", value["district"])
        district = value["district"]
        cur.execute("SELECT id FROM Districts WHERE district = %s", (district,))
        district_id = cur.fetchone()[0]

        # print("Zone:", value["zone"])
        zone = value["zone"]
        cur.execute(
            "INSERT INTO Zones(zone) VALUES(%s) ON CONFLICT (zone) DO NOTHING", (zone,)
        )
        cur.execute("SELECT id FROM Zones WHERE zone = %s", (zone,))
        zone_id = cur.fetchone()[0]

        cur.execute(
            "SELECT id FROM Cases WHERE state_id = %s AND district_id = %s",
            (state_id, district_id),
        )
        case_id = cur.fetchone()[0]

        cur.execute("UPDATE Cases SET zone_id=%s WHERE id=%s", (zone_id, case_id))

    conn.commit()
    # Update NULL Values
    cur.execute("SELECT id FROM Zones WHERE zone = %s", ("",))
    zone_id = cur.fetchone()[0]
    cur.execute("UPDATE Cases SET zone_id = %s WHERE zone_id is NULL", (zone_id,))

    # print("Updated Zone data.")
    conn.commit()
    cur.close()
    messages.add_message(
        request, messages.SUCCESS, "Succesfully Fetched Latest Data From the Server"
    )
    global vis_data
    vis_data = ""
    return redirect("app:getdata")


def getdata(request):
    conn = psycopg2.connect(
        database=database_name,
        user=database_user,
        host=database_host,
        port=database_port,
        password=database_password,
    )
    cursor = conn.cursor()
    cursor.execute('''
         SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'states' ) AS table_existence;
    ''')
    state_bool = cursor.fetchone()
    cursor.execute('''
         SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'districts' ) AS table_existence;
    ''')
    district_bool = cursor.fetchone()
    cursor.execute('''
         SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'cases' ) AS table_existence;
    ''')
    case_bool = cursor.fetchone()
    cursor.execute('''
         SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'zones' ) AS table_existence;
    ''')
    zone_bool = cursor.fetchone()

    if state_bool[0]==False or district_bool[0]==False or case_bool [0]==False or zone_bool [0]==False :
        fetch(request)
    cursor.execute("SELECT id, state FROM States")
    states = cursor.fetchall()

    cursor.execute("SELECT id, district FROM Districts")
    districts = cursor.fetchall()
    global vis_data
    if request.method == "POST":
        state_id = request.POST.get("state")
        district_id = request.POST.get("district")
        # print(states[int(state_id)-1][1])
        # print(district_id)
        for i in districts:
            if str(i[0]) == district_id:
                district = i[1]
                break
        cursor.execute(
            """
                    SELECT Zones.zone, Cases.confirmed, Cases.active, Cases.recovered, Cases.deceased
                    FROM Cases 
                    JOIN States 
                    ON Cases.state_id = States.id 
                     JOIN Districts
                    ON Cases.district_id = Districts.id 
                    JOIN Zones 
                    ON Cases.zone_id = Zones.id
                    WHERE States.state = %s AND
                    DistrictS.district  = %s
                    ORDER BY Cases.confirmed, Cases.active, Cases.recovered, Cases.deceased
                    """,
            (states[int(state_id) - 1][1], district),
        )

        covid_data = cursor.fetchone()
        # tableau_state_ids = ['viz1699272336150','viz1699272399492','viz1699272449653','viz1699272493570','viz1699272558205','viz1699272663003','viz1699272736951','viz1699272835962','viz1699272923197','viz1699272954795','viz1699273092907','viz1699273131939','viz1699273163397','viz1699273226955','viz1699273263804','viz1699273308247','viz1699273337681','viz1699273366331','viz1699273412768','viz1699273457077','viz1699273511804','viz1699273539469','viz1699273622550','viz1699273683071','viz1699273725026','viz1699273761984','viz1699273808065','viz1699273836864','viz1699273871695','viz1699273902429','viz1699273942376','viz1699273965892','viz1699273994038','viz1699274035988','viz1699274070290','viz1699274101871']
        # print(covid_data)

        vis_data = data[int(state_id) - 2]
        return render(
            request,
            "index.html",
            {
                "covid_data": covid_data,
                "state": states[int(state_id) - 1][1],
                "district": district,
                "getdata": True,
            },
        )

    # If it's a GET request, show the form to select the state and city
    cursor.execute(
        """
                  SELECT Districts.id, Districts.district
                    FROM Cases 
                    JOIN States
                    ON Cases.state_id = States.id
                     JOIN Districts 
                    ON Cases.district_id = Districts.id 
                    WHERE States.id = 2
                    """
    )
    districts = cursor.fetchall()
    conn.commit()
    conn.close()
    # print(districts)
    vis_data = ""
    return render(
        request,
        "index.html",
        {"states": states[1:], "districts": districts[1:], "selectdata": True},
    )


def visualize(request):
    if vis_data != "":
        return render(request, "index.html", {"vis_data": vis_data, "visualize": True})
    return render(request, "index.html", {"visualize": True})


def download(request):
    conn = psycopg2.connect(
        database=database_name,
        user=database_user,
        host=database_host,
        port=database_port,
        password=database_password,
    )
    cur = conn.cursor()

    fields = [
        "State",
        "District",
        "Zone",
        "Confirmed",
        "Active",
        "Recovered",
        "Deceased",
    ]

    response = HttpResponse(content_type="VARCHAR/csv")
    response["Content-Disposition"] = 'attachment; filename="covid_data.csv"'

    csvwriter = csv.writer(response)
    csvwriter.writerow(fields)

    cur.execute(
        """
        SELECT States.state, Districts.district, Zones.zone,
        Cases.confirmed, Cases.active, Cases.recovered, Cases.deceased 
        FROM Cases 
        JOIN States ON Cases.state_id = States.id
        JOIN Districts ON Cases.district_id = Districts.id
         JOIN Zones ON Cases.zone_id = Zones.id
        ORDER BY States.state, Districts.district
    """
    )

    for row in cur:
        state = row[0]
        districts = row[1]
        zones = row[2]
        confirmed = row[3]
        active = row[4]
        recovered = row[5]
        deceased = row[6]
        csvwriter.writerow(
            [state, districts, zones, confirmed, active, recovered, deceased]
        )
    messages.add_message(
        request, messages.SUCCESS, "Succesfully downloaded the CSV file!"
    )
    global vis_data
    vis_data = ""
    return response


def districts(request, state_id):
    # Fetch districts for the selected state

    conn = psycopg2.connect(
        database=database_name,
        user=database_user,
        host=database_host,
        port=database_port,
        password=database_password,
    )
    cursor = conn.cursor()
    cursor.execute(
        """SELECT Districts.id, Districts.district
                    FROM Cases 
                    JOIN States ON Cases.state_id = States.id 
                    JOIN Districts ON Cases.district_id = Districts.id 
                    WHERE States.id = %s 
                    """,
        (state_id,),
    )
    districts = [
        {"id": str(district[0]), "name": district[1]}
        for district in cursor.fetchall()[1:]
    ]
    conn.close()
    global vis_data
    vis_data = ""
    return JsonResponse(districts, safe=False)


# views.py
from django.shortcuts import render, redirect
from .forms import ContactFormModel


def contact_view(request):
    if request.method == "POST":
        form = ContactFormModel(request.POST)
        if form.is_valid():
            form.save()
            messages.add_message(
                request,
                messages.SUCCESS,
                "Thank you for your message. We will get back to you soon!",
            )
            return redirect("/")  # Redirect to a success page after form submission
    else:
        form = ContactFormModel()
    global vis_data
    vis_data = ""
    return render(request, "contact.html", {"form": form})
