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

# start_date = datetime(2024,10,29)
pasture_growth_rate = 65    #kg DM/ha/day
stock_consumption_rate = 14 #kg DM/animal/day

db_connection = None


def getCursor():
    """
    Gets a new dictionary cursor for the database.
    If not connected a new connection is established.

    Returns:
        cursor: A dictionary cursor object for executing database queries.
        None: If the connection cannot be established or an error occurs.
    """
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


def get_date():
    """
    Function read the date from the database table 'curr_date'.
    Returns:
        Dict: Return the curr_date from table
    """
    cursor = getCursor()        
    qstr = "SELECT curr_date FROM curr_date;"  
    cursor.execute(qstr)        
    result = cursor.fetchone()
    return result.get('curr_date')


####### Updated if statement with this line
@app.route("/")
def home():
    """
    API to render the home page of the application.
    Returns:
        render_template: The rendered HTML template for the home page 
    """
    curr_date = session.get('curr_date')
    # if curr_date not in session, update it to session
    if not curr_date:        
        curr_date = get_date()
        session.update({'curr_date': curr_date.strftime('%d %B %Y')})
    
    return render_template("home.html")


####### New function to reset the simulation back to the beginning - replaces reset_date() and clear_date()
##  NOTE: This requires fms-reset.sql file to be in the same folder as app.py
@app.route("/reset", methods=['GET'])
def reset():
    """
    API to reset data to original state.
    Returns:
        render_template: The rendered HTML template for the home page 
    """
    THIS_FOLDER = Path(__file__).parent.resolve()

    with open(THIS_FOLDER / 'fms-reset.sql', 'r') as f:
        mqstr = f.read()
        for qstr in mqstr.split(";"):
            cursor = getCursor()
            cursor.execute(qstr)
    
    # remove curr_date from session and update it from database
    session.pop('curr_date', None)
    flash("Data has been successfully reset to its original state.", "success")
    return redirect(url_for('home'))


@app.route("/clear_date", methods=['GET'])
def clear_date():
    """
    API to clear session data
    """
    curr_date = session.get('curr_date')
    if curr_date:
        session.pop('curr_date', None)
    flash("Date has been successfully removed from session.", "success")    
    return redirect(url_for('paddocks'))


@app.route('/next_day', methods=['GET'])
def next_day():
    """
    Increment the fms current date by one day and calculate the new paddock.
    Returns:
        render_template: The rendered HTML template for the home page
    """
    curr_date = get_date()
    # Add one day extra
    next_date = curr_date + timedelta(days=1)
    # update the new date in session
    session.update({'curr_date': next_date.strftime('%d %B %Y')})

    try:
        cursor = getCursor()
        # update the current date in the curr_date table
        cursor.execute("UPDATE curr_date SET curr_date = %s;", (next_date,))
        db_connection.commit()

        # update the paddock table with new pasture values
        qstr = """
        SELECT paddocks.id, paddocks.area, paddocks.dm_per_ha, paddocks.total_dm, COUNT(stock.id) AS stock_count
        FROM paddocks
        LEFT JOIN mobs ON paddocks.id = mobs.paddock_id
        LEFT JOIN stock ON mobs.id = stock.mob_id
        GROUP BY paddocks.id
        """
        cursor.execute(qstr)
        paddocks = cursor.fetchall()

        for paddock in paddocks:
            # Calculate new pasture levels
            calculated_pasture_level = pasture_levels(paddock)
            # Update paddock the paddock table with new pasture level
            cursor.execute("UPDATE paddocks SET total_dm = %s, dm_per_ha = %s  WHERE id = %s;", 
                        (calculated_pasture_level['total_dm'], calculated_pasture_level['dm_per_ha'], paddock.get('id')))
            db_connection.commit()
    except Exception as e:
        print("failed to connect with database", e)
    finally:
        cursor.close()

    return redirect(url_for('home'))
    

# Paddocks
@app.route("/paddocks")
def paddocks():
    """
    API to list the paddocks
    Returns:
        render_template: The rendered HTML template for the paddock page with paddock list
    """
    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect to database.", "danger")
        return "Failed to connect with database"
    
    qstr = """
    SELECT paddocks.id, paddocks.name AS paddock_name, paddocks.area, paddocks.dm_per_ha, paddocks.total_dm, mobs.name AS mob_name, COUNT(stock.id) AS stock_count
    FROM paddocks
    LEFT JOIN mobs ON paddocks.id = mobs.paddock_id
    LEFT JOIN stock ON mobs.id = stock.mob_id
    GROUP BY paddocks.id
    ORDER BY paddocks.name ASC;
    """
    try:
        cursor.execute(qstr)
        paddocks = cursor.fetchall()
    except Exception as e:
        flash("Failed to fetch data from paddock.", "danger")
        print("failed to fetch data from paddocks", e)
        paddocks = []
    finally:
        cursor.close()

    paddocks_detail = []
    # Highlight the paddock row with pasture level
    for paddock in paddocks:
        if paddock['dm_per_ha'] <= 1500:
            paddock['color'] = 'table-danger'
        elif paddock['dm_per_ha'] <= 1800:
            paddock['color'] = 'table-warning'
        else:
            paddock['color'] = ''
        
        paddocks_detail.append(paddock)

    return render_template("paddocks.html", paddocks=paddocks_detail)


@app.route('/add_paddock', methods=['POST'])
def add_paddock():
    """
    API to add a new paddock to the database.
    """
    name = request.form.get("name")
    area = request.form.get("area")
    dm_per_ha = request.form.get("dm_per_ha")

    if not name or not area or not dm_per_ha:
        flash("Please fill all fields.", "danger")
        redirect(url_for('paddocks'))

    area = round(float(area), 2)
    dm_per_ha = round(float(dm_per_ha), 2)
    total_dm = area * dm_per_ha
    total_dm = round(float(total_dm), 2)

    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect to database.", "danger")
        return "Failed to connect with database"
    
    try:
        # insert new paddock into paddocks table
        cursor.execute(
            "INSERT INTO paddocks (name, area, dm_per_ha, total_dm) VALUES (%s, %s, %s, %s)", 
            (name, area, dm_per_ha, total_dm)
        )
        db_connection.commit()
        flash("Paddock added successfully.", "success")
    except Exception as e:
        flash("Failed to add paddock to database.", "danger")
        return f"Failed to add paddock: {e}"
    finally:
        cursor.close()

    return redirect(url_for('paddocks'))


@app.route('/edit_paddock', methods=['POST'])
def edit_paddock():
    """
    API to edit a paddock in the database.        
    """
    paddock_id = request.form.get("id")
    name = request.form.get("name")
    area = request.form.get("area")
    dm_per_ha = request.form.get("dm_per_ha")

    if not paddock_id or not name or not area or not dm_per_ha:
        flash("Please fill all fields.", "danger")
        redirect(url_for('paddocks'))
    
    area = round(float(area), 2)
    dm_per_ha = round(float(dm_per_ha), 2)
    total_dm = area * dm_per_ha
    total_dm = round(float(total_dm), 2)

    cursor = getCursor()
    if cursor is None:
        return "Failed to connect with database"
    
    try:
        # search paddock with id present in table
        cursor.execute("SELECT id FROM paddocks WHERE id = %s", (paddock_id,))
        if not cursor.fetchone():
            flash("Paddock not found.", "danger")
            redirect(url_for('paddocks'))
            
        # update paddock in paddocks table
        cursor.execute("UPDATE paddocks SET name = %s, area = %s, dm_per_ha = %s , total_dm = %s WHERE id = %s;", 
                       (name, area, dm_per_ha, total_dm, paddock_id))
        db_connection.commit()
        flash("Paddock updated successfully.", "success")
        
    except Exception as e:
        flash("Failed to edit paddock in database.", "danger")
        print("failed to update paddocks", e)
    finally:
        cursor.close()
    
    return redirect(url_for('paddocks'))


@app.route('/delete_paddock/<int:paddock_id>', methods=['POST'])
def delete_paddock(paddock_id):
    """
    API to delete a paddock in the database.
    """
    cursor = getCursor()
    if cursor is None:
        return "Failed to connect with database"
    
    try:
        cursor.execute("SELECT COUNT(*) AS count FROM mobs WHERE paddock_id = %s", (paddock_id,))
        paddock_count = cursor.fetchone()

        # check paddock id in mob
        if paddock_count['count'] > 0:
            flash("Paddock cannot be deleted because it is assigned to a mob.", "danger")
            return redirect(url_for('paddocks'))

        cursor.execute("DELETE FROM paddocks WHERE id = %s", (paddock_id,))
        if cursor.rowcount == 0:
            flash("Paddock not found.", "danger")
            redirect(url_for('paddocks'))
        db_connection.commit()
        
    except Exception as e:
        print("failed to update paddocks", e)
    finally:
        cursor.close()

    flash("Paddock deleted successfully.", "success")
    return redirect(url_for('paddocks'))


# Mobs page
@app.route("/mobs")
def mobs():
    """
    API to list the mob details.
    Returns:
        render_template: The rendered HTML template for the mob page with mobslist
    """
    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect to database.", "danger")
        return redirect(url_for('mobs'))
    
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
        cursor.execute("SELECT id, name FROM paddocks;")
        paddocks = cursor.fetchall()
    except Exception as e:
        print("failed to fetch data from mobs", e)
        flash('Failed to fetch data from mobs', "danger")
        mobs = []
    finally:
        cursor.close()

    return render_template("mobs.html", mobs=mobs, paddocks=paddocks)


@app.route('/add_mob', methods=['POST'])
def add_mob():
    """
    API to add a new mob.
    """
    name = request.form.get("name")
    paddock_id = request.form.get("paddock_id")
    print(name, paddock_id)
    if not name or not paddock_id:
        flash("Please fill all fields.", "danger")
        return redirect(url_for('mobs'))

    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect to database.", "danger")
        return redirect(url_for('mobs'))
    
    try:
        cursor.execute("SELECT id FROM paddocks WHERE id = %s;", (paddock_id,))
        existing_paddock = cursor.fetchone()
        if not existing_paddock:
            flash("Paddock does not exist.", "danger")
            return redirect(url_for('mobs'))

        # check the paddock is not assign to any mob
        cursor.execute("SELECT name FROM mobs WHERE paddock_id = %s;", (paddock_id,))
        existing_mob = cursor.fetchone()
        if existing_mob:
            flash("Mob already exists in this paddock.", "danger")
            return redirect(url_for('mobs'))
        
        cursor.execute(
            "INSERT INTO mobs (name, paddock_id) VALUES (%s, %s)", 
            (name, paddock_id)
        )
        db_connection.commit()
        flash("Mob added successfully.", "success")
    except Exception as e:
        flash("Failed to add mob to database.", "danger")
        return redirect(url_for('mobs'))
    finally:
        cursor.close()
    
    return redirect(url_for('mobs'))


@app.route("/stocks")
def stocks():
    """
    API to list Stock by mob Group
    Returns:
        render_template: The rendered HTML template for the stock page with stock list
    """
    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect to database.", "danger")
        return redirect(url_for('stocks'))
    
    qstr = """
    SELECT stock.id, stock.dob, stock.weight, mobs.name AS mob_name, paddocks.name AS paddocks_name
    FROM stock
    LEFT JOIN mobs ON stock.mob_id = mobs.id
    LEFT JOIN paddocks ON paddocks.id = mobs.paddock_id
    ORDER BY mobs.name ASC, stock.id ASC;
    """
    try:
        cursor.execute(qstr)
        stocks = cursor.fetchall()
        cursor.execute("SELECT id, name FROM mobs;")
        mobs = cursor.fetchall()
    except Exception as e:
        flash("Failed to fetch stock from database.", "danger")
        print("failed to fetch data from stocks", e)
        stocks = []
    finally:
        cursor.close()

    # group the mob with same mob and paddock
    grouped_mob_data = {}
    for animal in stocks:
        mob_name = animal.get("mob_name")
        paddock_name = animal.get("paddocks_name")
        mob_paddock_key = (mob_name, paddock_name)

        if mob_paddock_key not in grouped_mob_data:
            grouped_mob_data[mob_paddock_key] = {
                "mob_name": mob_name,
                "paddock_name": paddock_name,
                "avg_weight": 0,
                "stock_count": 0,
                "stock": []
            }
        # increment the stock count by 1
        grouped_mob_data[mob_paddock_key]["stock_count"] += 1
        grouped_mob_data[mob_paddock_key]["avg_weight"] += animal.get("weight")
        grouped_mob_data[mob_paddock_key]["stock"].append({
            "id": animal.get("id"),
            "age": calculate_age(animal.get("dob")),
            "weight": animal.get("weight")
        })
    
    # calculate the average weight of total animals in a mob
    for mob_group in grouped_mob_data.values():
        mob_group["avg_weight"] /= mob_group["stock_count"]
        # round the caluated average weight to the nearest number
        mob_group["avg_weight"] = round(mob_group["avg_weight"])

    return render_template("stocks.html", grouped_mobs=list(grouped_mob_data.values()), mobs=mobs)


@app.route('/add_animal', methods=['POST'])
def add_animal():
    """
    Add a new animal to the database.
    """
    dob = request.form.get("dob")
    weight = request.form.get("weight")
    mob_id = request.form.get("mob_id")

    if not dob or not weight or not mob_id:
        flash("Please fill all fields.", "danger")
        return redirect(url_for('stocks'))

    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect to database.", "danger")
        return redirect(url_for('stocks'))
    
    try:
        cursor.execute("SELECT id FROM mobs WHERE id = %s;", (mob_id,))
        existing_mob = cursor.fetchone()
        if not existing_mob:
            flash("Mob does not exist.", "danger")
            return redirect(url_for('stocks'))
        
        cursor.execute(
            "INSERT INTO stock (mob_id, dob, weight) VALUES (%s, %s, %s)", 
            (mob_id, dob, weight)
        )
        db_connection.commit()
        flash("Stock added successfully.", "success")
    except Exception as e:
        flash("Failed to add stock to database.", "danger")
        return redirect(url_for('stocks'))
    finally:
        cursor.close()

    return redirect(url_for('stocks'))


@app.route('/get_mob_paddock', methods=['GET'])
def get_mob_paddock():
    """
    get mob and paddock
    Returns:
        render_template: The rendered HTML template for the move mob page with mobs list and paddock list
    """
    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect to database.", "danger")
        return "Failed to connect with database"
    
    try:
        cursor.execute("SELECT mobs.id AS mob_id, mobs.name AS mob_name FROM mobs ORDER BY mobs.name ASC;")
        mobs = cursor.fetchall()
        # Fetch the available paddock only
        cursor.execute("""
            SELECT paddocks.id AS paddock_id, paddocks.name AS paddock_name FROM paddocks 
            LEFT JOIN mobs ON paddocks.id = mobs.paddock_id
            WHERE mobs.paddock_id IS NULL
            ORDER BY paddocks.name ASC;
            """)
        paddocks = cursor.fetchall()
    except Exception as e:
        print("failed to fetch data from paddocks", e)
        flash("Failed to fetch data from database.", "danger")
        paddocks = []
        mobs = []
    finally:
        cursor.close()

    return render_template("move_mob.html", paddocks=paddocks, mobs=mobs)


@app.route('/move_mob', methods=['POST'])
def move_mob():
    """
    API to move one mob to another free paddock
    """
    print(request.form)
    mob_id = request.form.get("mob_id")
    paddock_id = request.form.get("paddock_id")

    if not mob_id or not paddock_id:
        flash("Please select the mobs and paddock to move mobs.", "danger")
        return "Please select the mobs and paddock to move mobs"
    
    cursor = getCursor()
    if cursor is None:
        flash("Failed to connect with database.", "danger")
        return "Failed to connect with database"
    
    try:
        cursor.execute("SELECT name FROM mobs WHERE id = %s", (mob_id,))
        mob_name = cursor.fetchone()
        if not mob_name:
            flash("Selected Mob not found.", "warning")
            return "Selected Mob not found."
        
        cursor.execute("SELECT name FROM paddocks WHERE id = %s", (paddock_id,))
        paddock_name = cursor.fetchone()
        if not paddock_name:
            flash("Selected Paddock not found.", "warning")
            return "Selected Paddock not found."

        # check if the paddock is available.
        cursor.execute("SELECT id FROM mobs WHERE paddock_id = %s;", (paddock_id,))
        existing_paddock = cursor.fetchone()
        if existing_paddock:
            flash("Selected Paddock not found.", "warning")
            return "Please select different paddock, it already contain mobs"

        # # move mob
        cursor.execute("UPDATE mobs SET paddock_id = %s WHERE id = %s;", (paddock_id, mob_id))
        db_connection.commit()
        
    except Exception as e:
        flash("Failed to move mob from paddock.", "danger")
        print("failed to fetch data from paddocks", e)
    finally:
        cursor.close()

    flash(f"{mob_name['name']} successfully moved to paddock {paddock_name['name']}.", "success")

    return redirect(url_for('get_mob_paddock'))


def pasture_levels(paddock):
    """
    Calculate total pasture (in kg DM) for a paddock based on area, growth rate and stock number.
    """
    area = paddock.get("area")
    stock_num = paddock.get("stock_count")
    total_dm = paddock.get("total_dm")
    growth = area * pasture_growth_rate
    consumption = stock_num * stock_consumption_rate
    total_dm = total_dm + growth - consumption
    dm_per_ha = total_dm / area
    return {'total_dm': round(total_dm, 2), 'dm_per_ha': round(dm_per_ha, 2)}

def calculate_age(birth_date):
    """
    Calculate the age of an animal from the fms date
    """
    current_date = get_date()
    age = (current_date.year - birth_date.year) - (
        (current_date.month, current_date.day) < (birth_date.month, birth_date.day)
    )

    return age