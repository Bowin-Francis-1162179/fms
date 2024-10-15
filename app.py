from flask import Flask, flash
from flask import render_template
from flask import request
from flask import redirect
from flask import url_for
from flask import session
from datetime import date, datetime, timedelta
import mysql.connector
import connect

####### Required for the reset function to work both locally and in PythonAnywhere
from pathlib import Path

app = Flask(__name__)
app.secret_key = 'COMP636 S2'

start_date = datetime(2024,10,29)
pasture_growth_rate = 65    #kg DM/ha/day
stock_consumption_rate = 14 #kg DM/animal/day

db_connection = None
 

def getCursor():
    """Gets a new dictionary cursor for the database.
    If necessary, a new database connection is created here and used for all
    subsequent to getCursor()."""
    try:
        global db_connection
        if db_connection is None or not db_connection.is_connected():
            db_connection = mysql.connector.connect(
                user=connect.dbuser, password=connect.dbpass, host=connect.dbhost,
                database=connect.dbname, autocommit=True
            )
        cursor = db_connection.cursor(dictionary=True, buffered=False)
        return cursor
    except Exception as e:
        print(f"Failed to connect with DB {e}")
        return None

####### New function - reads the date from the new database table.
def get_date():
    cursor = getCursor()        
    qstr = "SELECT curr_date FROM curr_date;"  
    cursor.execute(qstr)        
    result = cursor.fetchone()
    print(f" {result = }")  
    # result = {'curr_date': datetime.date(2024, 10, 29)}
    return result.get('curr_date')

####### Updated if statement with this line
@app.route("/")
def home():
    curr_date = session.get('curr_date')
    # if curr_date not in session, update it to session
    if not curr_date:        
        curr_date = get_date()
        session.update({'curr_date': curr_date.strftime('%Y-%m-%d')})

    # Format the date into Day Month Year
    curr_date_obj = datetime.strptime(session["curr_date"], "%Y-%m-%d").date()
    curr_date_formatted = curr_date_obj.strftime('%d %B %Y')

    return render_template("home.html", curr_date=curr_date_formatted)

####### New function to reset the simulation back to the beginning - replaces reset_date() and clear_date()
##  NOTE: This requires fms-reset.sql file to be in the same folder as app.py
@app.route("/reset", methods=['GET'])
def reset():
    """Reset data to original state."""
    THIS_FOLDER = Path(__file__).parent.resolve()

    with open(THIS_FOLDER / 'fms-reset.sql', 'r') as f:
        mqstr = f.read()
        for qstr in mqstr.split(";"):
            cursor = getCursor()
            cursor.execute(qstr)
    
    # remove curr_date from session and update it from database
    session.pop('curr_date', None)
    curr_date = get_date()
    
    return redirect(url_for('paddocks'))  

@app.route("/mobs")
def mobs():
    """
    List the mob details (excludes the stock in each mob).
    """
    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect with database")
        return "Failed to connect with database"
    
    qstr = """
    SELECT mobs.id, mobs.name AS mob_name, paddocks.name AS paddock_name, COUNT(stock.id) AS stock_count FROM mobs 
    LEFT JOIN paddocks ON mobs.paddock_id = paddocks.id 
    LEFT JOIN stock ON mobs.id = stock.mob_id
    GROUP BY mobs.id
    ORDER BY mobs.name ASC;
    """
    try:
        cursor.execute(qstr)
        mobs = cursor.fetchall()
        print(f"{ mobs = }")
    except Exception as e:
        print("failed to fetch data from mobs", e)
        flash('Failed to fetch data from mobs')
        mobs = []
    finally:
        cursor.close()

    return render_template("mobs.html", mobs=mobs)


@app.route("/paddocks")
def paddocks():
    """List paddock details."""
    return render_template("paddocks.html")  


