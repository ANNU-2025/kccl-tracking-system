from flask import Flask, render_template, request, redirect, url_for, session, flash, send_file, jsonify
import psycopg2
from psycopg2 import pool
import pandas as pd
from datetime import datetime
from io import BytesIO
import warnings
import os
import threading
import traceback

warnings.filterwarnings('ignore')
os.environ['TZ'] = 'Asia/Kolkata'

app = Flask(__name__)
app.secret_key = 'AnuragChat007'

DB_URL = "postgresql://postgres.kvagszyqyqzizjtzpdcv:AnuragChat007@aws-1-ap-northeast-1.pooler.supabase.com:6543/postgres"

connection_pool = None
db_lock = threading.Lock()
db_initialized = False


def _force_ensure_columns(conn):
    cols = [
        ("material_logs", "serial_nos", "TEXT"),
        ("material_logs", "item_name", "TEXT"),
        ("consumable_logs", "item_name", "TEXT"),
        ("consumable_stock", "item_name", "TEXT"),
    ]
    for table, col, dtype in cols:
        try:
            c = conn.cursor()
            c.execute("SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s", (table, col))
            if not c.fetchone():
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {dtype}")
                conn.commit()
                print(f"✅ Added {col} to {table}")
            c.close()
        except:
            try: conn.rollback()
            except: pass


def init_db():
    global connection_pool, db_initialized
    if db_initialized:
        return True
    with db_lock:
        if db_initialized:
            return True
        try:
            pool_inst = psycopg2.pool.SimpleConnectionPool(1, 10, dsn=DB_URL, connect_timeout=10)
            test_conn = pool_inst.getconn()
            test_cur = test_conn.cursor()
            test_cur.execute("SELECT 1")
            test_cur.close()
            _force_ensure_columns(test_conn)
            pool_inst.putconn(test_conn)
            connection_pool = pool_inst
            db_initialized = True
            print("✅ Database Connected!")
            return True
        except Exception as e:
            print(f"❌ DB Init Error: {e}")
            db_initialized = False
            return False


def get_db():
    if not init_db():
        return None
    try:
        conn = connection_pool.getconn()
        if conn:
            cur = conn.cursor()
            cur.execute("SET TIME ZONE 'Asia/Kolkata'")
            cur.close()
            _force_ensure_columns(conn)
        return conn
    except:
        global db_initialized
        db_initialized = False
        return None


def release_db(conn):
    if conn and connection_pool:
        try: connection_pool.putconn(conn)
        except: pass


def fix_timezone(df):
    for col in df.select_dtypes(include=['datetimetz', 'datetime64[ns, UTC]']).columns:
        df[col] = df[col].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
    return df


def safe_dt(val):
    if val is None:
        return datetime.min
    if hasattr(val, 'tzinfo') and val.tzinfo is not None:
        return val.replace(tzinfo=None)
    return val


def dl_excel(df, filename):
    df = fix_timezone(df)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(output, download_name=filename, as_attachment=True)


@app.context_processor
def inject_now():
    return {'now': datetime.now()}


# ==================== LOGIN ====================

@app.route('/', methods=['GET', 'POST'])
def login():
    try:
        if request.method == 'POST':
            u = request.form.get('username')
            p = request.form.get('password')
            conn = get_db()
            if not conn:
                flash('Database connection failed!', 'error')
                return render_template('login.html')
            cur = conn.cursor()
            cur.execute("SELECT username, role FROM users WHERE username=%s AND password=%s", (u, p))
            res = cur.fetchone()
            cur.close()
            release_db(conn)
            if res:
                session['logged_user'] = res[0]
                session['user_role'] = (res[1] or 'user').lower()
                return redirect(url_for('dashboard'))
            flash('Invalid Credentials!', 'error')
        return render_template('login.html')
    except Exception as e:
        print(traceback.format_exc())
        return f"Error: {str(e)}", 500


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ==================== DASHBOARD ====================

@app.route('/dashboard')
def dashboard():
    if 'logged_user' not in session:
        return redirect(url_for('login'))
    conn = get_db()
    if not conn:
        flash('Database connection failed', 'error')
        return redirect(url_for('login'))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM stb_stock WHERE status='In Stock'")
    total_in_stock = cur.fetchone()[0] or 0
    cur.execute("SELECT COUNT(*) FROM stb_stock WHERE status='In Stock' AND stock_type='Returned'")
    ret_c = cur.fetchone()[0] or 0
    fresh_c = total_in_stock - ret_c
    cur.execute("SELECT status, COUNT(*) FROM stb_stock GROUP BY status")
    status_counts = dict(cur.fetchall())
    issued_c = status_counts.get('Issued', 0)
    faulty_c = status_counts.get('Faulty', 0)
    cur.execute("SELECT COALESCE(SUM(balance_length), 0) FROM fibre_stock")
    fib_val = cur.fetchone()[0] or 0
    material_summary = []
    try:
        cur.execute("""SELECT ms.item_code, COALESCE(mm.item_name, ms.item_code),
                   SUM(CASE WHEN ms.status='In Stock' THEN 1 ELSE 0 END)
                   FROM material_serials ms LEFT JOIN material_master mm ON ms.item_code = mm.item_code
                   GROUP BY ms.item_code, mm.item_name ORDER BY ms.item_code""")
        material_summary = cur.fetchall()
    except: pass
    consumable_summary = []
    try:
        cur.execute("""SELECT item_code, COALESCE(NULLIF(item_name,''), item_code),
                   SUM(total_qty), SUM(used_qty), SUM(balance_qty)
                   FROM consumable_stock GROUP BY item_code, item_name ORDER BY item_code""")
        consumable_summary = cur.fetchall()
    except: pass
    dealer_data = []
    try:
        cur.execute("""SELECT dealer, COUNT(*) FROM stb_stock
                   WHERE status='Issued' AND dealer IS NOT NULL AND dealer != ''
                   GROUP BY dealer ORDER BY dealer ASC LIMIT 50""")
        dealer_data = cur.fetchall()
    except: pass
    cur.close()
    release_db(conn)
    return render_template('dashboard.html', total_in_stock=total_in_stock, fresh_c=fresh_c, ret_c=ret_c,
        issued_c=issued_c, faulty_c=faulty_c, fib_val=fib_val, material_summary=material_summary,
        consumable_summary=consumable_summary, dealer_data=dealer_data)


# ==================== STB ====================

@app.route('/stb', methods=['GET', 'POST'])
def stb_manager():
    if 'logged_user' not in session:
        return redirect(url_for('login'))
    if request.method == 'POST':
        sno = request.form.get('stb_no', '').upper()
        dlr = request.form.get('dealer', '').upper()
        act = request.form.get('action')
        conn = get_db()
        if not conn:
            flash('Database connection failed', 'error')
            return render_template('stb_manager.html')
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM stb_stock WHERE stb_no=%s", (sno,))
            if not cur.fetchone():
                cur.execute("INSERT INTO stb_stock (stb_no,status,stock_type,created_at) VALUES (%s,'In Stock','Fresh',NOW())", (sno,))
            if act == "Issue":
                cur.execute("UPDATE stb_stock SET status='Issued',dealer=%s,updated_at=NOW() WHERE stb_no=%s", (dlr, sno))
            elif act == "Return":
                cur.execute("UPDATE stb_stock SET status='In Stock',dealer=NULL,stock_type='Returned',updated_at=NOW() WHERE stb_no=%s", (sno,))
            elif act == "Faulty":
                cur.execute("UPDATE stb_stock SET status='Faulty',dealer=NULL,updated_at=NOW() WHERE stb_no=%s", (sno,))
            cur.execute("INSERT INTO stb_logs (stb_no,action,dealer,done_by,created_at) VALUES (%s,%s,%s,%s,NOW())", (sno, act, dlr, session['logged_user']))
            conn.commit()
            flash('STB Transaction Successful!', 'success')
        except Exception as e:
            conn.rollback()
            flash(f'Error: {e}', 'error')
        finally:
            cur.close()
            release_db(conn)
    return render_template('stb_manager.html')


@app.route('/stb/bulk', methods=['POST'])
def stb_bulk():
    if 'logged_user' not in session:
        return redirect(url_for('login'))
    act = request.form.get('bulk_action')
    dlr = request.form.get('bulk_dealer', '').upper()
    file = request.files.get('file')
    if file and act:
        try:
            df = pd.read_csv(file, dtype=str)
            conn = get_db()
            if not conn:
                return redirect(url_for('stb_manager'))
            cur = conn.cursor()
            count = 0
            for _, row in df.iterrows():
                sn = str(row.iloc[0]).strip().upper()
                if not sn: continue
                if "Add New" in act:
                    cur.execute("SELECT 1 FROM stb_stock WHERE stb_no=%s", (sn,))
                    if not cur.fetchone():
                        cur.execute("INSERT INTO stb_stock (stb_no,status,stock_type,created_at) VALUES (%s,'In Stock','Fresh',NOW())", (sn,))
                        cur.execute("INSERT INTO stb_logs (stb_no,action,dealer,done_by,created_at) VALUES (%s,%s,%s,%s,NOW())", (sn, "Add New", dlr, session['logged_user']))
                        count += 1
                else:
                    cur.execute("SELECT 1 FROM stb_stock WHERE stb_no=%s", (sn,))
                    if not cur.fetchone():
                        cur.execute("INSERT INTO stb_stock (stb_no,status,stock_type,created_at) VALUES (%s,'In Stock','Fresh',NOW())", (sn,))
                    if "Issue" in act:
                        cur.execute("UPDATE stb_stock SET status='Issued',dealer=%s,updated_at=NOW() WHERE stb_no=%s", (dlr, sn))
                    elif "Return" in act:
                        cur.execute("UPDATE stb_stock SET status='In Stock',dealer=NULL,stock_type='Returned',updated_at=NOW() WHERE stb_no=%s", (sn,))
                    elif "Faulty" in act:
                        cur.execute("UPDATE stb_stock SET status='Faulty',dealer=NULL,updated_at=NOW() WHERE stb_no=%s", (sn,))
                    cur.execute("INSERT INTO stb_logs (stb_no,action,dealer,done_by,created_at) VALUES (%s,%s,%s,%s,NOW())", (sn, act, dlr, session['logged_user']))
                    count += 1
            conn.commit()
            flash(f'{count} Records Processed!', 'success')
        except Exception as e:
            flash(f'Bulk Error: {e}', 'error')
        finally:
            try: cur.close(); release_db(conn)
            except: pass
    return redirect(url_for('stb_manager'))


@app.route('/stb/search')
def stb_search():
    term = request.args.get('q', '')
    conn = get_db()
    if not conn:
        return jsonify({'found': False})
    cur = conn.cursor()
    cur.execute("SELECT stb_no,status,dealer,stock_type FROM stb_stock WHERE stb_no=%s", (term,))
    res = cur.fetchone()
    cur.close()
    release_db(conn)
    if res:
        return jsonify({'found': True, 'data': {'sn': res[0], 'status': res[1], 'dealer': res[2] or 'N/A', 'type': res[3]}})
    return jsonify({'found': False})


# ==================== ITEM LOOKUP ====================

@app.route('/item/lookup')
def item_lookup():
    if 'logged_user' not in session:
        return jsonify({})
    code = request.args.get('code', '').strip().upper()
    if not code:
        return jsonify({})
    conn = get_db()
    if not conn:
        return jsonify({})
    cur = conn.cursor()
    result = {}
    try:
        cur.execute("SELECT item_name FROM material_master WHERE item_code=%s", (code,))
        row = cur.fetchone()
        if row and row[0]:
            result['name'] = row[0]
    except: pass
    if not result:
        try:
            cur.execute("SELECT item_name FROM consumable_stock WHERE item_code=%s ORDER BY id DESC LIMIT 1", (code,))
            row = cur.fetchone()
            if row and row[0]:
                result['name'] = row[0]
        except: pass
    cur.close()
    release_db(conn)
    return jsonify(result)


# ==================== INVENTORY TEMPLATE DOWNLOAD ====================

@app.route('/inventory/template/<item_cat>')
def inventory_template(item_cat):
    if 'logged_user' not in session:
        return redirect(url_for('login'))
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        if item_cat == 'consumable':
            df = pd.DataFrame(columns=['item_code', 'item_name', 'batch_id', 'quantity', 'unit', 'invoice_no'])
            df.to_excel(writer, sheet_name="Consumable Template", index=False)
        else:
            df = pd.DataFrame(columns=['item_code', 'item_name', 'serial_no', 'quantity', 'invoice_no'])
            df.to_excel(writer, sheet_name="Material Template", index=False)
    output.seek(0)
    fname = "Consumable_Bulk_Template.xlsx" if item_cat == 'consumable' else "Material_Bulk_Template.xlsx"
    return send_file(output, download_name=fname, as_attachment=True)


# ==================== INVENTORY BULK ====================

@app.route('/inventory/bulk', methods=['POST'])
def inventory_bulk():
    if 'logged_user' not in session:
        return redirect(url_for('login'))
    file = request.files.get('bulk_file')
    item_cat = request.form.get('item_category', 'material')
    bulk_action = request.form.get('bulk_action', 'add')
    dealer = request.form.get('bulk_dealer', '').upper()
    if not file:
        flash('Please select a file', 'error')
        return redirect(url_for('inventory'))
    try:
        if file.filename.endswith('.xlsx'):
            df = pd.read_excel(file, dtype=str)
        else:
            df = pd.read_csv(file, dtype=str)
        if df.empty:
            flash('File is empty', 'error')
            return redirect(url_for('inventory'))
        # Clean column names
        df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
        conn = get_db()
        if not conn:
            flash('Database connection failed', 'error')
            return redirect(url_for('inventory'))
        cur = conn.cursor()
        count = 0
        errors = 0

        for _, row in df.iterrows():
            try:
                if item_cat == 'material':
                    code = str(row.get('item_code', '')).strip().upper()
                    name = str(row.get('item_name', '')).strip().upper()
                    serial = str(row.get('serial_no', '')).strip().upper()
                    qty_str = str(row.get('quantity', '')).strip()
                    invoice = str(row.get('invoice_no', '')).strip().upper()
                    if not code:
                        errors += 1
                        continue
                    if serial and serial not in ('', 'NAN', 'NONE'):
                        qty = 1
                    elif qty_str and qty_str not in ('', 'NAN', 'NONE'):
                        qty = int(float(qty_str))
                    else:
                        qty = 1
                    cur.execute("SELECT 1 FROM material_master WHERE item_code=%s", (code,))
                    if not cur.fetchone():
                        cur.execute("INSERT INTO material_master (item_code,item_name) VALUES (%s,%s)", (code, name))
                    elif name:
                        cur.execute("UPDATE material_master SET item_name=%s WHERE item_code=%s AND (item_name IS NULL OR item_name='')", (name, code))
                    serial_nos_list = []
                    if bulk_action == 'add':
                        for i in range(qty):
                            if serial and qty == 1:
                                s = serial
                            elif serial and qty > 1:
                                s = f"{serial}_{i+1}"
                            else:
                                s = f"{code}_{datetime.now().strftime('%f')}_{i}"
                            cur.execute("INSERT INTO material_serials (serial_no,item_code,status,created_at) VALUES (%s,%s,'In Stock',NOW())", (s, code))
                            serial_nos_list.append(s)
                    elif bulk_action == 'issue':
                        if serial:
                            cur.execute("UPDATE material_serials SET status='Issued',dealer=%s,updated_at=NOW() WHERE serial_no=%s AND status='In Stock'", (dealer, serial))
                            serial_nos_list.append(serial)
                        else:
                            cur.execute("SELECT serial_no FROM material_serials WHERE item_code=%s AND status='In Stock' ORDER BY created_at ASC LIMIT %s", (code, qty))
                            for r in cur.fetchall():
                                cur.execute("UPDATE material_serials SET status='Issued',dealer=%s,updated_at=NOW() WHERE serial_no=%s", (dealer, r[0]))
                                serial_nos_list.append(r[0])
                    elif bulk_action == 'return':
                        if serial:
                            cur.execute("UPDATE material_serials SET status='In Stock',dealer=NULL,updated_at=NOW() WHERE serial_no=%s AND status='Issued'", (serial,))
                            serial_nos_list.append(serial)
                        else:
                            cur.execute("SELECT serial_no FROM material_serials WHERE item_code=%s AND status='Issued' ORDER BY updated_at DESC LIMIT %s", (code, qty))
                            for r in cur.fetchall():
                                cur.execute("UPDATE material_serials SET status='In Stock',dealer=NULL,updated_at=NOW() WHERE serial_no=%s", (r[0],))
                                serial_nos_list.append(r[0])
                    serial_nos_str = ','.join(serial_nos_list) if serial_nos_list else None
                    action_label = {'add': 'Add New', 'issue': 'Issue', 'return': 'Return'}[bulk_action]
                    try:
                        cur.execute("INSERT INTO material_logs (item_code,item_name,action,quantity,dealer,invoice_no,done_by,serial_nos,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                                    (code, name, action_label, qty, dealer, invoice, session['logged_user'], serial_nos_str))
                    except:
                        cur.execute("INSERT INTO material_logs (item_code,action,quantity,dealer,invoice_no,done_by,serial_nos,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())",
                                    (code, action_label, qty, dealer, invoice, session['logged_user'], serial_nos_str))

                else:  # CONSUMABLE
                    code = str(row.get('item_code', '')).strip().upper()
                    name = str(row.get('item_name', '')).strip().upper()
                    batch = str(row.get('batch_id', '')).strip().upper()
                    qty_str = str(row.get('quantity', '')).strip()
                    unit = str(row.get('unit', 'Pcs')).strip()
                    invoice = str(row.get('invoice_no', '')).strip().upper()
                    if not code or not batch:
                        errors += 1
                        continue
                    qty = float(qty_str) if qty_str and qty_str not in ('', 'NAN', 'NONE') else 0
                    action_label = {'add': 'Add New', 'issue': 'Issue', 'return': 'return'}[bulk_action]
                    if bulk_action == 'add':
                        cur.execute("SELECT 1 FROM consumable_stock WHERE batch_id=%s", (batch,))
                        if cur.fetchone():
                            errors += 1
                            continue
                        cur.execute("INSERT INTO consumable_stock (item_code,item_name,batch_id,unit,total_qty,used_qty,balance_qty) VALUES (%s,%s,%s,%s,%s,0,%s)", (code, name, batch, unit, qty, qty))
                    elif bulk_action == 'issue':
                        cur.execute("UPDATE consumable_stock SET used_qty=used_qty+%s,balance_qty=balance_qty-%s WHERE batch_id=%s", (qty, qty, batch))
                    elif bulk_action == 'return':
                        cur.execute("UPDATE consumable_stock SET used_qty=used_qty-%s,balance_qty=balance_qty+%s WHERE batch_id=%s", (qty, qty, batch))
                    if name:
                        cur.execute("UPDATE consumable_stock SET item_name=%s WHERE batch_id=%s AND (item_name IS NULL OR item_name='')", (name, batch))
                    try:
                        cur.execute("INSERT INTO consumable_logs (item_code,item_name,batch_id,action,qty,dealer,invoice_no,done_by,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                                    (code, name, batch, action_label, qty, dealer, invoice, session['logged_user']))
                    except:
                        cur.execute("INSERT INTO consumable_logs (item_code,batch_id,action,qty,dealer,invoice_no,done_by,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())",
                                    (code, batch, action_label, qty, dealer, invoice, session['logged_user']))
                count += 1
            except Exception as e:
                print(f"Bulk row error: {e}")
                errors += 1

        conn.commit()
        msg = f'{count} {item_cat.title()} items processed'
        if errors:
            msg += f', {errors} skipped'
        flash(msg, 'success')
    except Exception as e:
        flash(f'Bulk Error: {str(e)}', 'error')
    finally:
        try: cur.close(); release_db(conn)
        except: pass
    return redirect(url_for('inventory'))


# ==================== INVENTORY ====================

@app.route('/inventory', methods=['GET', 'POST'])
def inventory():
    if 'logged_user' not in session:
        return redirect(url_for('login'))
    conn = get_db()
    if not conn:
        flash('Database connection failed', 'error')
        return redirect(url_for('dashboard'))
    cur = conn.cursor()
    if request.method == 'POST':
        form_type = request.form.get('form_type')
        try:
            if form_type == 'material':
                code = request.form.get('m_c', '').upper()
                name = request.form.get('m_n', '').upper()
                serial = request.form.get('m_s', '').upper()
                qty = int(request.form.get('m_qty', 1))
                dlr = request.form.get('m_d', '').upper()
                inv = request.form.get('m_invoice', '').upper()
                act = request.form.get('m_act')
                cur.execute("SELECT 1 FROM material_master WHERE item_code=%s", (code,))
                if not cur.fetchone():
                    cur.execute("INSERT INTO material_master (item_code,item_name) VALUES (%s,%s)", (code, name))
                elif name:
                    cur.execute("UPDATE material_master SET item_name=%s WHERE item_code=%s", (name, code))
                serial_nos_list = []
                if act == 'Add New':
                    for i in range(qty):
                        s = serial if (serial and qty == 1) else f"{code}_{datetime.now().strftime('%f')}_{i}"
                        cur.execute("INSERT INTO material_serials (serial_no,item_code,status,created_at) VALUES (%s,%s,'In Stock',NOW())", (s, code))
                        serial_nos_list.append(s)
                        try: cur.execute("INSERT INTO material_stock (item_code,item_name,quantity,serial_no) VALUES (%s,%s,1,%s)", (code, name, s))
                        except: pass
                elif act == 'Issue':
                    if serial:
                        cur.execute("UPDATE material_serials SET status='Issued',dealer=%s,updated_at=NOW() WHERE serial_no=%s", (dlr, serial))
                        serial_nos_list.append(serial)
                        try: cur.execute("DELETE FROM material_stock WHERE serial_no=%s", (serial,))
                        except: pass
                    else:
                        cur.execute("SELECT serial_no FROM material_serials WHERE item_code=%s AND status='In Stock' ORDER BY created_at ASC LIMIT %s", (code, qty))
                        for r in cur.fetchall():
                            cur.execute("UPDATE material_serials SET status='Issued',dealer=%s,updated_at=NOW() WHERE serial_no=%s", (dlr, r[0]))
                            serial_nos_list.append(r[0])
                            try: cur.execute("DELETE FROM material_stock WHERE serial_no=%s", (r[0],))
                            except: pass
                elif act == 'Return':
                    if serial:
                        cur.execute("UPDATE material_serials SET status='In Stock',dealer=NULL,updated_at=NOW() WHERE serial_no=%s", (serial,))
                        serial_nos_list.append(serial)
                        try: cur.execute("INSERT INTO material_stock (item_code,item_name,quantity,serial_no) VALUES (%s,%s,1,%s)", (code, name, serial))
                        except: pass
                    else:
                        cur.execute("SELECT serial_no FROM material_serials WHERE item_code=%s AND status='Issued' ORDER BY updated_at DESC LIMIT %s", (code, qty))
                        for r in cur.fetchall():
                            cur.execute("UPDATE material_serials SET status='In Stock',dealer=NULL,updated_at=NOW() WHERE serial_no=%s", (r[0],))
                            serial_nos_list.append(r[0])
                            try: cur.execute("INSERT INTO material_stock (item_code,item_name,quantity,serial_no) VALUES (%s,%s,1,%s)", (code, name, r[0]))
                            except: pass
                serial_nos_str = ','.join(serial_nos_list) if serial_nos_list else None
                try:
                    cur.execute("INSERT INTO material_logs (item_code,item_name,action,quantity,dealer,invoice_no,done_by,serial_nos,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                                (code, name, act, qty, dlr, inv, session['logged_user'], serial_nos_str))
                except:
                    cur.execute("INSERT INTO material_logs (item_code,action,quantity,dealer,invoice_no,done_by,serial_nos,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())",
                                (code, act, qty, dlr, inv, session['logged_user'], serial_nos_str))
                conn.commit()
                flash('Hardware Transaction Successful!', 'success')

            elif form_type == 'consumable':
                item = request.form.get('c_item', '').upper()
                item_name = request.form.get('c_name', '').upper()
                batch = request.form.get('c_batch', '').upper()
                qty = float(request.form.get('c_qty', 0))
                unit = request.form.get('c_unit')
                dlr = request.form.get('c_dealer', '').upper()
                inv = request.form.get('c_invoice', '').upper()
                act = request.form.get('c_action')
                if act == 'Add New':
                    cur.execute("SELECT 1 FROM consumable_stock WHERE batch_id=%s", (batch,))
                    if cur.fetchone():
                        flash('Batch ID exists!', 'error')
                    else:
                        cur.execute("INSERT INTO consumable_stock (item_code,item_name,batch_id,unit,total_qty,used_qty,balance_qty) VALUES (%s,%s,%s,%s,%s,0,%s)", (item, item_name, batch, unit, qty, qty))
                elif act == 'Issue':
                    cur.execute("UPDATE consumable_stock SET used_qty=used_qty+%s,balance_qty=balance_qty-%s WHERE batch_id=%s", (qty, qty, batch))
                elif act == 'Return':
                    cur.execute("UPDATE consumable_stock SET used_qty=used_qty-%s,balance_qty=balance_qty+%s WHERE batch_id=%s", (qty, qty, batch))
                if item_name:
                    cur.execute("UPDATE consumable_stock SET item_name=%s WHERE batch_id=%s AND (item_name IS NULL OR item_name='')", (item_name, batch))
                if not item_name:
                    cur.execute("SELECT item_name FROM consumable_stock WHERE batch_id=%s", (batch,))
                    stock_row = cur.fetchone()
                    if stock_row and stock_row[0]:
                        item_name = stock_row[0]
                try:
                    cur.execute("INSERT INTO consumable_logs (item_code,item_name,batch_id,action,qty,dealer,invoice_no,done_by,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                                (item, item_name, batch, act, qty, dlr, inv, session['logged_user']))
                except:
                    cur.execute("INSERT INTO consumable_logs (item_code,batch_id,action,qty,dealer,invoice_no,done_by,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())",
                                (item, batch, act, qty, dlr, inv, session['logged_user']))
                conn.commit()
                flash('Consumable Transaction Successful!', 'success')
        except Exception as e:
            conn.rollback()
            flash(f'Error: {str(e)}', 'error')
            print(traceback.format_exc())
    cur.close()
    release_db(conn)
    return render_template('inventory.html')


# ==================== FIBRE ====================

@app.route('/fibre', methods=['GET', 'POST'])
def fibre_manager():
    if 'logged_user' not in session:
        return redirect(url_for('login'))
    conn = get_db()
    if not conn:
        flash('Database connection failed', 'error')
        return render_template('fibre.html')
    cur = conn.cursor()
    if request.method == 'POST':
        did = request.form.get('drum_id', '').upper()
        lg = request.form.get('length', '0')
        dlr = request.form.get('lco_name', '').upper()
        act = request.form.get('action')
        try:
            lg_val = float(lg or 0)
            if act == 'Add New':
                cur.execute("INSERT INTO fibre_stock (drum_id,total_length,used_length,balance_length) VALUES (%s,%s,0,%s)", (did, lg_val, lg_val))
            elif act == 'Issue':
                cur.execute("UPDATE fibre_stock SET used_length=used_length+%s,balance_length=balance_length-%s WHERE drum_id=%s", (lg_val, lg_val, did))
            elif act == 'Return':
                cur.execute("UPDATE fibre_stock SET used_length=used_length-%s,balance_length=balance_length+%s WHERE drum_id=%s", (lg_val, lg_val, did))
            cur.execute("INSERT INTO fibre_logs (drum_id,action,length,dealer,done_by,created_at) VALUES (%s,%s,%s,%s,%s,NOW())", (did, act, lg_val, dlr, session['logged_user']))
            conn.commit()
            flash('Fibre Transaction Successful!', 'success')
        except Exception as e:
            conn.rollback()
            flash(f'Error: {e}', 'error')
    cur.close()
    release_db(conn)
    return render_template('fibre.html')


# ==================== LOGS ====================

@app.route('/logs')
def logs():
    if 'logged_user' not in session:
        return redirect(url_for('login'))
    conn = get_db()
    if not conn:
        flash('Database connection failed', 'error')
        return redirect(url_for('dashboard'))
    cur = conn.cursor()
    f_date = request.args.get('from_date', datetime.now().strftime('%Y-%m-01'))
    t_date = request.args.get('to_date', datetime.now().strftime('%Y-%m-%d'))
    search_term = request.args.get('search', '')
    combined = []
    try:
        q = """SELECT 'Material', ml.item_code, ml.quantity, ml.action, ml.dealer, ml.invoice_no, ml.done_by,
                      COALESCE(ml.serial_nos, ''), COALESCE(NULLIF(ml.item_name,''), ml.item_code), ml.created_at
               FROM material_logs ml WHERE DATE(ml.created_at)>=%s AND DATE(ml.created_at)<=%s"""
        p = [f_date, t_date]
        if search_term:
            q += " AND (ml.item_code LIKE %s OR ml.dealer LIKE %s OR ml.invoice_no LIKE %s OR ml.serial_nos LIKE %s)"
            p += [f"%{search_term}%"] * 4
        q += " ORDER BY ml.created_at DESC"
        cur.execute(q, tuple(p))
        combined.extend(cur.fetchall())
    except:
        try:
            q = """SELECT 'Material', ml.item_code, ml.quantity, ml.action, ml.dealer, ml.invoice_no, ml.done_by,
                          COALESCE(ml.serial_nos, ''), ml.item_code, ml.created_at
                   FROM material_logs ml WHERE DATE(ml.created_at)>=%s AND DATE(ml.created_at)<=%s"""
            p = [f_date, t_date]
            if search_term:
                q += " AND (ml.item_code LIKE %s OR ml.dealer LIKE %s OR ml.invoice_no LIKE %s)"
                p += [f"%{search_term}%"] * 3
            q += " ORDER BY ml.created_at DESC"
            cur.execute(q, tuple(p))
            combined.extend(cur.fetchall())
        except: pass
    try:
        q = """SELECT 'Consumable', cl.item_code, cl.qty, cl.action, cl.dealer, cl.invoice_no, cl.done_by,
                      '', COALESCE(NULLIF(cl.item_name,''), cl.item_code), cl.created_at
               FROM consumable_logs cl WHERE DATE(COALESCE(cl.created_at,NOW()))>=%s AND DATE(COALESCE(cl.created_at,NOW()))<=%s"""
        p = [f_date, t_date]
        if search_term:
            q += " AND (cl.item_code LIKE %s OR cl.dealer LIKE %s OR cl.invoice_no LIKE %s OR cl.item_name LIKE %s)"
            p += [f"%{search_term}%"] * 4
        q += " ORDER BY cl.created_at DESC"
        cur.execute(q, tuple(p))
        combined.extend(cur.fetchall())
    except:
        try:
            q = """SELECT 'Consumable', cl.item_code, cl.qty, cl.action, cl.dealer, cl.invoice_no, cl.done_by,
                          '', cl.item_code, cl.created_at
                   FROM consumable_logs cl WHERE DATE(COALESCE(cl.created_at,NOW()))>=%s AND DATE(COALESCE(cl.created_at,NOW()))<=%s"""
            p = [f_date, t_date]
            if search_term:
                q += " AND (cl.item_code LIKE %s OR cl.dealer LIKE %s OR cl.invoice_no LIKE %s)"
                p += [f"%{search_term}%"] * 3
            q += " ORDER BY cl.created_at DESC"
            cur.execute(q, tuple(p))
            combined.extend(cur.fetchall())
        except: pass
    try:
        combined.sort(key=lambda x: safe_dt(x[9]), reverse=True)
    except: pass
    stb_logs = []
    try:
        q = "SELECT stb_no,action,dealer,done_by,created_at FROM stb_logs WHERE DATE(created_at)>=%s AND DATE(created_at)<=%s"
        p = [f_date, t_date]
        if search_term:
            q += " AND (stb_no LIKE %s OR dealer LIKE %s)"
            p += [f"%{search_term}%"] * 2
        q += " ORDER BY created_at DESC"
        cur.execute(q, tuple(p))
        stb_logs = cur.fetchall()
    except: pass
    fibre_logs = []
    try:
        q = "SELECT drum_id,action,length,dealer,done_by,created_at FROM fibre_logs WHERE DATE(created_at)>=%s AND DATE(created_at)<=%s"
        p = [f_date, t_date]
        if search_term:
            q += " AND (drum_id LIKE %s OR dealer LIKE %s)"
            p += [f"%{search_term}%"] * 2
        q += " ORDER BY created_at DESC"
        cur.execute(q, tuple(p))
        fibre_logs = cur.fetchall()
    except: pass
    cur.close()
    release_db(conn)
    return render_template('logs.html', combined=combined, stb_logs=stb_logs, fibre_logs=fibre_logs, f_date=f_date, t_date=t_date, search_term=search_term)


# ==================== EXPORTS ====================

@app.route('/export/instock')
def export_instock():
    if 'logged_user' not in session:
        return redirect(url_for('dashboard'))
    conn = get_db()
    if not conn:
        return redirect(url_for('dashboard'))
    output = BytesIO()
    try:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            try:
                df = pd.read_sql("SELECT stb_no as \"Serial No\", stock_type as \"Type\", updated_at as \"Last Update\" FROM stb_stock WHERE status='In Stock'", conn)
                if not df.empty: df = fix_timezone(df); df.to_excel(writer, sheet_name="STB Stock", index=False)
            except: pass
            try:
                df = pd.read_sql("""SELECT COALESCE(NULLIF(ml.item_name,''), ml.item_code) as "Item Name",
                       ml.item_code as "Item Code", COALESCE(ml.serial_nos,'') as "Serial Nos",
                       ml.quantity as "Quantity", 'Pcs' as "Unit", ml.dealer as "Dealer",
                       ml.invoice_no as "Invoice", ml.action as "Action", ml.created_at as "Date"
                       FROM material_logs ml ORDER BY ml.created_at DESC""", conn)
                if not df.empty: df = fix_timezone(df); df.to_excel(writer, sheet_name="Hardware Log", index=False)
            except:
                try:
                    df = pd.read_sql("""SELECT ml.item_code as "Item Name", ml.item_code as "Item Code",
                           COALESCE(ml.serial_nos,'') as "Serial Nos", ml.quantity as "Quantity",
                           'Pcs' as "Unit", ml.dealer as "Dealer", ml.invoice_no as "Invoice",
                           ml.action as "Action", ml.created_at as "Date"
                           FROM material_logs ml ORDER BY ml.created_at DESC""", conn)
                    if not df.empty: df = fix_timezone(df); df.to_excel(writer, sheet_name="Hardware Log", index=False)
                except: pass
            try:
                df = pd.read_sql("""SELECT cl.item_code as "Item Code",
                       COALESCE(NULLIF(cl.item_name,''), cl.item_code) as "Item Name",
                       cl.batch_id as "Batch ID", cl.qty as "Quantity", cs.unit as "Unit",
                       cl.dealer as "Dealer", cl.invoice_no as "Invoice", cl.action as "Action",
                       cl.created_at as "Date"
                       FROM consumable_logs cl LEFT JOIN consumable_stock cs ON cl.batch_id=cs.batch_id
                       ORDER BY cl.created_at DESC""", conn)
                if not df.empty: df = fix_timezone(df); df.to_excel(writer, sheet_name="Consumable Log", index=False)
            except:
                try:
                    df = pd.read_sql("""SELECT cl.item_code as "Item Code", cl.item_code as "Item Name",
                           cl.batch_id as "Batch ID", cl.qty as "Quantity", cs.unit as "Unit",
                           cl.dealer as "Dealer", cl.invoice_no as "Invoice", cl.action as "Action",
                           cl.created_at as "Date"
                           FROM consumable_logs cl LEFT JOIN consumable_stock cs ON cl.batch_id=cs.batch_id
                           ORDER BY cl.created_at DESC""", conn)
                    if not df.empty: df = fix_timezone(df); df.to_excel(writer, sheet_name="Consumable Log", index=False)
                except: pass
    except Exception as e:
        flash(f"Export Error: {e}", "error")
        release_db(conn)
        return redirect(url_for('dashboard'))
    output.seek(0)
    release_db(conn)
    return send_file(output, download_name="KCCL_InStock_Full_Report.xlsx", as_attachment=True)


@app.route('/export/stb/<status>')
def export_stb_status(status):
    if 'logged_user' not in session:
        return redirect(url_for('dashboard'))
    conn = get_db()
    if not conn: return redirect(url_for('dashboard'))
    try:
        df = pd.read_sql("SELECT stb_no as \"Serial No\", status as \"Status\", stock_type as \"Type\", dealer as \"Dealer\", updated_at as \"Last Update\" FROM stb_stock WHERE status=%s", conn, params=(status,))
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, f"KCCL_{status}_Report.xlsx")
    except Exception as e:
        flash(f"Export Error: {e}", "error"); release_db(conn)
        return redirect(url_for('dashboard'))


@app.route('/export/dealer/<dealer_name>')
def export_dealer(dealer_name):
    if 'logged_user' not in session:
        return redirect(url_for('dashboard'))
    conn = get_db()
    if not conn: return redirect(url_for('dashboard'))
    try:
        df = pd.read_sql("SELECT stb_no as \"Serial No\", dealer as \"Dealer\", stock_type as \"Type\", updated_at as \"Last Update\" FROM stb_stock WHERE dealer=%s AND status='Issued'", conn, params=(dealer_name,))
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, f"STB_Issued_{dealer_name}.xlsx")
    except Exception as e:
        flash(f"Export Error: {e}", "error"); release_db(conn)
        return redirect(url_for('dashboard'))


@app.route('/export/hardware')
def export_hardware():
    if 'logged_user' not in session:
        return redirect(url_for('dashboard'))
    conn = get_db()
    if not conn: return redirect(url_for('dashboard'))
    try:
        df = pd.read_sql("""SELECT COALESCE(NULLIF(ml.item_name,''), ml.item_code) as "Item Name",
               ml.item_code as "Item Code", COALESCE(ml.serial_nos,'') as "Serial Nos",
               ml.quantity as "Quantity", 'Pcs' as "Unit", ml.dealer as "Dealer",
               ml.invoice_no as "Invoice", ml.action as "Action", ml.created_at as "Date"
               FROM material_logs ml ORDER BY ml.created_at DESC""", conn)
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, "Material_Transaction_Report.xlsx")
    except:
        try:
            df = pd.read_sql("""SELECT ml.item_code as "Item Name", ml.item_code as "Item Code",
                   COALESCE(ml.serial_nos,'') as "Serial Nos", ml.quantity as "Quantity",
                   'Pcs' as "Unit", ml.dealer as "Dealer", ml.invoice_no as "Invoice",
                   ml.action as "Action", ml.created_at as "Date"
                   FROM material_logs ml ORDER BY ml.created_at DESC""", conn)
            df = fix_timezone(df); release_db(conn)
            return dl_excel(df, "Material_Transaction_Report.xlsx")
        except Exception as e:
            flash(f"Export Error: {e}", "error"); release_db(conn)
            return redirect(url_for('dashboard'))


@app.route('/export/fibre')
def export_fibre():
    if 'logged_user' not in session:
        return redirect(url_for('dashboard'))
    conn = get_db()
    if not conn: return redirect(url_for('dashboard'))
    try:
        df = pd.read_sql("""SELECT fl.drum_id as "Drum ID", fl.action as "Action",
               fl.length as "Length (M)", fl.dealer as "Dealer", fl.done_by as "Done By",
               fl.created_at as "Date Time" FROM fibre_logs fl ORDER BY fl.created_at DESC""", conn)
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, "Fibre_Transaction_Report.xlsx")
    except Exception as e:
        flash(f"Export Error: {e}", "error"); release_db(conn)
        return redirect(url_for('dashboard'))


@app.route('/export/consumables')
def export_consumables():
    if 'logged_user' not in session:
        return redirect(url_for('dashboard'))
    conn = get_db()
    if not conn: return redirect(url_for('dashboard'))
    try:
        df = pd.read_sql("""SELECT cl.item_code as "Item Code",
               COALESCE(NULLIF(cl.item_name,''), cl.item_code) as "Item Name",
               cl.batch_id as "Batch ID", cl.qty as "Quantity", cs.unit as "Unit",
               cl.dealer as "Dealer", cl.invoice_no as "Invoice", cl.action as "Action",
               cl.created_at as "Date"
               FROM consumable_logs cl LEFT JOIN consumable_stock cs ON cl.batch_id=cs.batch_id
               ORDER BY cl.created_at DESC""", conn)
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, "Consumable_Transaction_Report.xlsx")
    except:
        try:
            df = pd.read_sql("""SELECT cl.item_code as "Item Code", cl.item_code as "Item Name",
                   cl.batch_id as "Batch ID", cl.qty as "Quantity", cs.unit as "Unit",
                   cl.dealer as "Dealer", cl.invoice_no as "Invoice", cl.action as "Action",
                   cl.created_at as "Date"
                   FROM consumable_logs cl LEFT JOIN consumable_stock cs ON cl.batch_id=cs.batch_id
                   ORDER BY cl.created_at DESC""", conn)
            df = fix_timezone(df); release_db(conn)
            return dl_excel(df, "Consumable_Transaction_Report.xlsx")
        except Exception as e:
            flash(f"Export Error: {e}", "error"); release_db(conn)
            return redirect(url_for('dashboard'))


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
