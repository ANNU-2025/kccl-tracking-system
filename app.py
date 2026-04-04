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


def _clean(val):
    if val is None: return ''
    s = str(val).strip()
    if s.lower() in ('nan', 'none', 'nat', ''): return ''
    return s


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
    if db_initialized: return True
    with db_lock:
        if db_initialized: return True
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
    if not init_db(): return None
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
    if val is None: return datetime.min
    if hasattr(val, 'tzinfo') and val.tzinfo is not None: return val.replace(tzinfo=None)
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
                   GROUP BY dealer ORDER BY dealer ASC""")
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
            if not conn: return redirect(url_for('stb_manager'))
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
    if not conn: return jsonify({'found': False})
    cur = conn.cursor()
    cur.execute("SELECT stb_no,status,dealer,stock_type FROM stb_stock WHERE stb_no=%s", (term,))
    res = cur.fetchone()
    cur.close()
    release_db(conn)
    if res:
        return jsonify({'found': True, 'data': {'sn': res[0], 'status': res[1], 'dealer': res[2] or 'N/A', 'type': res[3]}})
    return jsonify({'found': False})


@app.route('/item/lookup')
def item_lookup():
    if 'logged_user' not in session: return jsonify({})
    code = request.args.get('code', '').strip().upper()
    if not code: return jsonify({})
    conn = get_db()
    if not conn: return jsonify({})
    cur = conn.cursor()
    result = {}
    try:
        cur.execute("SELECT item_name FROM material_master WHERE item_code=%s", (code,))
        row = cur.fetchone()
        if row and row[0]: result['name'] = row[0]
    except: pass
    if not result:
        try:
            cur.execute("SELECT item_name FROM consumable_stock WHERE item_code=%s ORDER BY id DESC LIMIT 1", (code,))
            row = cur.fetchone()
            if row and row[0]: result['name'] = row[0]
        except: pass
    cur.close()
    release_db(conn)
    return jsonify(result)


# ==================== TEMPLATE DOWNLOAD ====================

@app.route('/inventory/template/<item_cat>')
def inventory_template(item_cat):
    if 'logged_user' not in session: return redirect(url_for('login'))
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
        df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
        df = df.dropna(how='all')

        conn = get_db()
        if not conn:
            flash('Database connection failed', 'error')
            return redirect(url_for('inventory'))
        cur = conn.cursor()
        count = 0
        failed_rows = []

        for idx, row in df.iterrows():
            row_num = idx + 2
            try:
                if item_cat == 'material':
                    code = _clean(row.get('item_code', '')).upper()
                    name = _clean(row.get('item_name', '')).upper()
                    serial = _clean(row.get('serial_no', '')).upper()
                    qty_str = _clean(row.get('quantity', ''))
                    invoice = _clean(row.get('invoice_no', '')).upper()

                    if not code:
                        raise ValueError("Item Code is empty")

                    if serial:
                        qty = 1
                    elif qty_str:
                        try:
                            qty = int(float(qty_str))
                            if qty <= 0:
                                raise ValueError(f"Invalid quantity: {qty_str}")
                        except ValueError:
                            raise ValueError(f"Invalid quantity: {qty_str}")
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
                            if cur.rowcount == 0:
                                raise ValueError(f"Serial '{serial}' not found in stock or already issued")
                            serial_nos_list.append(serial)
                        else:
                            cur.execute("SELECT serial_no FROM material_serials WHERE item_code=%s AND status='In Stock' ORDER BY created_at ASC LIMIT %s", (code, qty))
                            found = cur.fetchall()
                            if not found:
                                raise ValueError(f"No 'In Stock' items found for {code}")
                            for r in found:
                                cur.execute("UPDATE material_serials SET status='Issued',dealer=%s,updated_at=NOW() WHERE serial_no=%s", (dealer, r[0]))
                                serial_nos_list.append(r[0])
                    elif bulk_action == 'return':
                        if serial:
                            cur.execute("UPDATE material_serials SET status='In Stock',dealer=NULL,updated_at=NOW() WHERE serial_no=%s AND status='Issued'", (serial,))
                            if cur.rowcount == 0:
                                raise ValueError(f"Serial '{serial}' not found as issued")
                            serial_nos_list.append(serial)
                        else:
                            cur.execute("SELECT serial_no FROM material_serials WHERE item_code=%s AND status='Issued' ORDER BY updated_at DESC LIMIT %s", (code, qty))
                            found = cur.fetchall()
                            if not found:
                                raise ValueError(f"No 'Issued' items found for {code}")
                            for r in found:
                                cur.execute("UPDATE material_serials SET status='In Stock',dealer=NULL,updated_at=NOW() WHERE serial_no=%s", (r[0],))
                                serial_nos_list.append(r[0])

                    # ★ KEY FIX: if qty > 1, store blank serial_nos in log
                    serial_nos_str = ','.join(serial_nos_list) if serial_nos_list and qty <= 1 else ''
                    action_label = {'add': 'Add New', 'issue': 'Issue', 'return': 'Return'}[bulk_action]
                    try:
                        cur.execute("INSERT INTO material_logs (item_code,item_name,action,quantity,dealer,invoice_no,done_by,serial_nos,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                                    (code, name, action_label, qty, dealer, invoice, session['logged_user'], serial_nos_str))
                    except:
                        cur.execute("INSERT INTO material_logs (item_code,action,quantity,dealer,invoice_no,done_by,serial_nos,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())",
                                    (code, action_label, qty, dealer, invoice, session['logged_user'], serial_nos_str))

                else:  # CONSUMABLE
                    code = _clean(row.get('item_code', '')).upper()
                    name = _clean(row.get('item_name', '')).upper()
                    batch = _clean(row.get('batch_id', '')).upper()
                    qty_str = _clean(row.get('quantity', ''))
                    unit = _clean(row.get('unit', '')) or 'Pcs'
                    invoice = _clean(row.get('invoice_no', '')).upper()

                    if not code:
                        raise ValueError("Item Code is empty")
                    if not batch:
                        raise ValueError("Batch ID is empty")
                    if not qty_str:
                        raise ValueError("Quantity is empty")
                    try:
                        qty = float(qty_str)
                        if qty <= 0:
                            raise ValueError(f"Invalid quantity: {qty_str}")
                    except ValueError:
                        raise ValueError(f"Invalid quantity: {qty_str}")

                    action_label = {'add': 'Add New', 'issue': 'Issue', 'return': 'return'}[bulk_action]
                    if bulk_action == 'add':
                        cur.execute("SELECT 1 FROM consumable_stock WHERE batch_id=%s", (batch,))
                        if cur.fetchone():
                            raise ValueError(f"Batch ID '{batch}' already exists")
                        cur.execute("INSERT INTO consumable_stock (item_code,item_name,batch_id,unit,total_qty,used_qty,balance_qty) VALUES (%s,%s,%s,%s,%s,0,%s)", (code, name, batch, unit, qty, qty))
                    elif bulk_action == 'issue':
                        cur.execute("UPDATE consumable_stock SET used_qty=used_qty+%s,balance_qty=balance_qty-%s WHERE batch_id=%s", (qty, qty, batch))
                        if cur.rowcount == 0:
                            raise ValueError(f"Batch ID '{batch}' not found")
                    elif bulk_action == 'return':
                        cur.execute("UPDATE consumable_stock SET used_qty=used_qty-%s,balance_qty=balance_qty+%s WHERE batch_id=%s", (qty, qty, batch))
                        if cur.rowcount == 0:
                            raise ValueError(f"Batch ID '{batch}' not found")

                    if name:
                        cur.execute("UPDATE consumable_stock SET item_name=%s WHERE batch_id=%s AND (item_name IS NULL OR item_name='')", (name, batch))

                    try:
                        cur.execute("INSERT INTO consumable_logs (item_code,item_name,batch_id,action,qty,dealer,invoice_no,done_by,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())",
                                    (code, name, batch, action_label, qty, dealer, invoice, session['logged_user']))
                    except:
                        cur.execute("INSERT INTO consumable_logs (item_code,batch_id,action,qty,dealer,invoice_no,done_by,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())",
                                    (code, batch, action_label, qty, dealer, invoice, session['logged_user']))

                conn.commit()
                count += 1

            except Exception as e:
                try:
                    conn.rollback()
                except:
                    pass
                err_msg = str(e)
                if len(err_msg) > 120:
                    err_msg = err_msg[:120] + '...'
                failed_rows.append({
                    'row': row_num,
                    'code': _clean(row.get('item_code', '')),
                    'name': _clean(row.get('item_name', '')),
                    'error': err_msg
                })

        session['bulk_failures'] = failed_rows[-50:]

        msg = f'{count} {item_cat.title()} items processed successfully'
        if failed_rows:
            msg += f', {len(failed_rows)} failed'
        flash(msg, 'success' if failed_rows == 0 else 'error')

    except Exception as e:
        flash(f'Bulk Error: {str(e)}', 'error')
        session['bulk_failures'] = []
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
    inv_results = []
    inv_search = ''

    bulk_failures = session.pop('bulk_failures', [])

    if request.method == 'POST':
        form_type = request.form.get('form_type')
        inv_search_post = request.form.get('inv_search', '').strip()

        if inv_search_post and not form_type:
            inv_search = inv_search_post
            try:
                cur.execute("""SELECT 'Material', ml.item_code, 
                           CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos,'') END,
                           ml.quantity, 'Pcs', ml.dealer, ml.action,
                           COALESCE(NULLIF(ml.item_name,''), ml.item_code), ml.created_at
                           FROM material_logs ml WHERE ml.invoice_no=%s""", (inv_search,))
                inv_results.extend(cur.fetchall())
            except:
                try:
                    cur.execute("""SELECT 'Material', ml.item_code, 
                               CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos,'') END,
                               ml.quantity, 'Pcs', ml.dealer, ml.action,
                               ml.item_code, ml.created_at
                               FROM material_logs ml WHERE ml.invoice_no=%s""", (inv_search,))
                    inv_results.extend(cur.fetchall())
                except: pass
            try:
                cur.execute("""SELECT 'Consumable', cl.item_code, cl.batch_id,
                           cl.qty, cs.unit, cl.dealer, cl.action,
                           COALESCE(NULLIF(cl.item_name,''), cl.item_code), cl.created_at
                           FROM consumable_logs cl LEFT JOIN consumable_stock cs ON cl.batch_id = cs.batch_id
                           WHERE cl.invoice_no=%s""", (inv_search,))
                inv_results.extend(cur.fetchall())
            except:
                try:
                    cur.execute("""SELECT 'Consumable', cl.item_code, cl.batch_id,
                               cl.qty, cs.unit, cl.dealer, cl.action,
                               cl.item_code, cl.created_at
                               FROM consumable_logs cl LEFT JOIN consumable_stock cs ON cl.batch_id = cs.batch_id
                               WHERE cl.invoice_no=%s""", (inv_search,))
                    inv_results.extend(cur.fetchall())
                except: pass

        else:
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
                    # ★ KEY FIX: blank serial_nos when qty > 1
                    serial_nos_str = ','.join(serial_nos_list) if serial_nos_list and qty <= 1 else ''
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
                        if stock_row and stock_row[0]: item_name = stock_row[0]
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

    # Invoice search via GET
    if not inv_search:
        inv_search = request.args.get('inv_search', '').strip()
        if inv_search:
            try:
                cur.execute("""SELECT 'Material', ml.item_code, 
                           CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos,'') END,
                           ml.quantity, 'Pcs', ml.dealer, ml.action,
                           COALESCE(NULLIF(ml.item_name,''), ml.item_code), ml.created_at
                           FROM material_logs ml WHERE ml.invoice_no=%s""", (inv_search,))
                inv_results.extend(cur.fetchall())
            except:
                try:
                    cur.execute("""SELECT 'Material', ml.item_code, 
                               CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos,'') END,
                               ml.quantity, 'Pcs', ml.dealer, ml.action,
                               ml.item_code, ml.created_at
                               FROM material_logs ml WHERE ml.invoice_no=%s""", (inv_search,))
                    inv_results.extend(cur.fetchall())
                except: pass
            try:
                cur.execute("""SELECT 'Consumable', cl.item_code, cl.batch_id,
                           cl.qty, cs.unit, cl.dealer, cl.action,
                           COALESCE(NULLIF(cl.item_name,''), cl.item_code), cl.created_at
                           FROM consumable_logs cl LEFT JOIN consumable_stock cs ON cl.batch_id = cs.batch_id
                           WHERE cl.invoice_no=%s""", (inv_search,))
                inv_results.extend(cur.fetchall())
            except:
                try:
                    cur.execute("""SELECT 'Consumable', cl.item_code, cl.batch_id,
                               cl.qty, cs.unit, cl.dealer, cl.action,
                               cl.item_code, cl.created_at
                               FROM consumable_logs cl LEFT JOIN consumable_stock cs ON cl.batch_id = cs.batch_id
                               WHERE cl.invoice_no=%s""", (inv_search,))
                    inv_results.extend(cur.fetchall())
                except: pass

    cur.close()
    release_db(conn)
    return render_template('inventory.html', inv_results=inv_results, inv_search=inv_search, bulk_failures=bulk_failures)


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
                      CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos, '') END, 
                      COALESCE(NULLIF(ml.item_name,''), ml.item_code), ml.created_at
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
                          CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos, '') END,
                          ml.item_code, ml.created_at
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
    if 'logged_user' not in session: return redirect(url_for('dashboard'))
    conn = get_db()
    if not conn: return redirect(url_for('dashboard'))
    output = BytesIO()
    try:
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            try:
                df = pd.read_sql("SELECT stb_no as \"Serial No\", stock_type as \"Type\", updated_at as \"Last Update\" FROM stb_stock WHERE status='In Stock'", conn)
                if not df.empty: df = fix_timezone(df); df.to_excel(writer, sheet_name="STB Stock", index=False)
            except: pass
            try:
                df = pd.read_sql("""SELECT COALESCE(NULLIF(ml.item_name,''), ml.item_code) as "Item Name",
                       ml.item_code as "Item Code", 
                       CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos,'') END as "Serial Nos",
                       ml.quantity as "Quantity", 'Pcs' as "Unit", ml.dealer as "Dealer",
                       ml.invoice_no as "Invoice", ml.action as "Action", ml.created_at as "Date"
                       FROM material_logs ml ORDER BY ml.created_at DESC""", conn)
                if not df.empty: df = fix_timezone(df); df.to_excel(writer, sheet_name="Hardware Log", index=False)
            except:
                try:
                    df = pd.read_sql("""SELECT ml.item_code as "Item Name", ml.item_code as "Item Code",
                           CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos,'') END as "Serial Nos",
                           ml.quantity as "Quantity", 'Pcs' as "Unit", ml.dealer as "Dealer",
                           ml.invoice_no as "Invoice", ml.action as "Action", ml.created_at as "Date"
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
    if 'logged_user' not in session: return redirect(url_for('dashboard'))
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
    if 'logged_user' not in session: return redirect(url_for('dashboard'))
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
    if 'logged_user' not in session: return redirect(url_for('dashboard'))
    conn = get_db()
    if not conn: return redirect(url_for('dashboard'))
    try:
        df = pd.read_sql("""SELECT COALESCE(NULLIF(ml.item_name,''), ml.item_code) as "Item Name",
               ml.item_code as "Item Code", 
               CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos,'') END as "Serial Nos",
               ml.quantity as "Quantity", 'Pcs' as "Unit", ml.dealer as "Dealer",
               ml.invoice_no as "Invoice", ml.action as "Action", ml.created_at as "Date"
               FROM material_logs ml ORDER BY ml.created_at DESC""", conn)
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, "Material_Transaction_Report.xlsx")
    except:
        try:
            df = pd.read_sql("""SELECT ml.item_code as "Item Name", ml.item_code as "Item Code",
                   CASE WHEN ml.quantity > 1 THEN '' ELSE COALESCE(ml.serial_nos,'') END as "Serial Nos",
                   ml.quantity as "Quantity", 'Pcs' as "Unit", ml.dealer as "Dealer",
                   ml.invoice_no as "Invoice", ml.action as "Action", ml.created_at as "Date"
                   FROM material_logs ml ORDER BY ml.created_at DESC""", conn)
            df = fix_timezone(df); release_db(conn)
            return dl_excel(df, "Material_Transaction_Report.xlsx")
        except Exception as e:
            flash(f"Export Error: {e}", "error"); release_db(conn)
            return redirect(url_for('dashboard'))


@app.route('/export/fibre')
def export_fibre():
    if 'logged_user' not in session: return redirect(url_for('dashboard'))
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
    if 'logged_user' not in session: return redirect(url_for('dashboard'))
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

# ==================== DAILY ACTIVE SUMMARY ====================

def _export_compare_base(d_from, d_to, mode, area, sub_dist, dist, is_growth):
    if not d_from or not d_to or d_from == d_to: return None, "Invalid dates"
    conn = get_db()
    if not conn: return None, "DB error"
    cur = conn.cursor()
    try:
        q = """SELECT COALESCE(t1.lco_code,t2.lco_code), COALESCE(lm.lco_name,COALESCE(t1.lco_code,t2.lco_code)),
               COALESCE(lm.area,''), COALESCE(lm.sub_distributor,''), COALESCE(lm.distributor,''),
               COALESCE(t1.active_count,0), COALESCE(t2.active_count,0),
               COALESCE(t1.deactive_count,0), COALESCE(t2.deactive_count,0)
               FROM (SELECT lco_code,active_count,deactive_count FROM daily_active_summary WHERE report_date=%s) t1
               FULL OUTER JOIN (SELECT lco_code,active_count,deactive_count FROM daily_active_summary WHERE report_date=%s) t2
               ON t1.lco_code=t2.lco_code
               LEFT JOIN lco_master lm ON COALESCE(t1.lco_code,t2.lco_code)=lm.lco_code"""
        p = [d_from, d_to]
        if area: q += " AND lm.area=%s"; p.append(area)
        if sub_dist: q += " AND lm.sub_distributor=%s"; p.append(sub_dist)
        cur.execute(q, tuple(p))
        rows = cur.fetchall()
        cur.close()
        release_db(conn)
        data = []
        for r in rows:
            if mode == 'active': change = r[6] - r[5]; prev_v, now_v = r[5], r[6]
            else: change = r[7] - r[8]; prev_v, now_v = r[7], r[8]
            if is_growth and change > 0: data.append({'LCO Code': r[0], 'LCO Name': r[1], 'Area': r[2], 'Prev': prev_v, 'Now': now_v, 'Change': change})
            elif not is_growth and change < 0: data.append({'LCO Code': r[0], 'LCO Name': r[1], 'Area': r[2], 'Prev': prev_v, 'Now': now_v, 'Change': change})
        if not data: return None, None
        return pd.DataFrame(data), None
    except Exception as e:
        try: cur.close()
        except: pass
        release_db(conn)
        return None, str(e)

@app.route('/daily-active', methods=['GET', 'POST'])
def daily_active():
    if 'logged_user' not in session: return redirect(url_for('login'))
    conn = get_db()
    if not conn: flash('Database connection failed', 'error'); return redirect(url_for('dashboard'))
    cur = conn.cursor()
    if request.method == 'POST':
        form_type = request.form.get('form_type', '')
        if form_type == 'bulk':
            file = request.files.get('da_file')
            if not file: flash('Please select a file', 'error')
            else:
                try:
                    if file.filename.endswith('.xlsx'): df = pd.read_excel(file, dtype=str)
                    else: df = pd.read_csv(file, dtype=str, encoding='utf-8-sig')
                    if df.empty: flash('File is empty', 'error')
                    else:
                        df.columns = [str(c).strip().lower().replace(' ', '_').replace('\ufeff', '') for c in df.columns]
                        count = 0; failed = []
                        for idx, row in df.iterrows():
                            rn = idx + 2
                            try:
                                raw_date = _clean(row.get('report_date', ''))
                                lco_name = _clean(row.get('lco_name', '')).upper()
                                act_str = _clean(row.get('active_count', ''))
                                deact_str = _clean(row.get('deactive_count', '0') or '0')
                                dist_val = _clean(row.get('distributor', ''))
                                if not raw_date or not lco_name or not act_str: raise ValueError("Missing required field")
                                parsed_date = None
                                for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d-%m-%Y', '%d/%m/%Y', '%m-%d-%Y', '%Y/%m/%d'):
                                    try: parsed_date = datetime.strptime(raw_date, fmt).date(); break
                                    except: continue
                                if not parsed_date: raise ValueError("Invalid date: " + raw_date)
                                act = int(float(act_str)); deact = int(float(deact_str))
                                cur.execute("SELECT lco_code FROM lco_master WHERE UPPER(lco_name) = %s", (lco_name,))
                                cr = cur.fetchone()
                                if cr: lco_code = cr[0]
                                else:
                                    lco_code = lco_name.replace(' ', '_')[:50]
                                    try: cur.execute("INSERT INTO lco_master (lco_code, lco_name, distributor) VALUES (%s, %s, %s) ON CONFLICT (lco_code) DO NOTHING", (lco_code, lco_name, dist_val)); conn.commit()
                                    except: pass
                                if dist_val:
                                    try: cur.execute("UPDATE lco_master SET distributor = %s WHERE lco_code = %s AND (distributor IS NULL OR distributor = '')", (dist_val, lco_code)); conn.commit()
                                    except: pass
                                try: cur.execute("INSERT INTO daily_active_summary (report_date, lco_code, active_count, deactive_count) VALUES (%s,%s,%s,%s) ON CONFLICT (report_date, lco_code) DO UPDATE SET active_count=EXCLUDED.active_count, deactive_count=EXCLUDED.deactive_count", (parsed_date, lco_code, act, deact))
                                except: cur.execute("INSERT INTO daily_active_summary (report_date, lco_code, active_count, deactive_count) VALUES (%s,%s,%s,%s)", (parsed_date, lco_code, act, deact))
                                conn.commit(); count += 1
                            except Exception as e:
                                try: conn.rollback()
                                except: pass
                                failed.append({'row': rn, 'name': _clean(row.get('lco_name', '')), 'error': str(e)[:100]})
                        msg = f'{count} records uploaded'
                        if failed: msg += f', {len(failed)} failed'
                        flash(msg, 'success' if not failed else 'error'); session['da_failures'] = failed[-50:]
                except Exception as e: flash(f'Upload Error: {e}', 'error'); session['da_failures'] = []
    cur.execute("SELECT DISTINCT area FROM lco_master WHERE area IS NOT NULL AND area != '' ORDER BY area")
    areas = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT sub_distributor FROM lco_master WHERE sub_distributor IS NOT NULL AND sub_distributor != '' ORDER BY sub_distributor")
    sub_dists = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT distributor FROM lco_master WHERE distributor IS NOT NULL AND distributor != '' ORDER BY distributor")
    distributors = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT DISTINCT report_date FROM daily_active_summary ORDER BY report_date DESC")
    date_list = [r[0] for r in cur.fetchall()]
    bulk_failures = session.pop('da_failures', [])
    cur.close(); release_db(conn)
    return render_template('daily_active.html', areas=areas, sub_dists=sub_dists, distributors=distributors, date_list=date_list, bulk_failures=bulk_failures)


@app.route('/daily-active/sub-dists')
def da_sub_dists():
    if 'logged_user' not in session: return jsonify([])
    area = request.args.get('area', ''); dist = request.args.get('distributor', '')
    conn = get_db()
    if not conn: return jsonify([])
    cur = conn.cursor()
    q = "SELECT DISTINCT sub_distributor FROM lco_master WHERE sub_distributor IS NOT NULL AND sub_distributor != ''"
    p = []
    if area: q += " AND area = %s"; p.append(area)
    if dist: q += " AND COALESCE(distributor,'') = %s"; p.append(dist)
    q += " ORDER BY sub_distributor"
    try: cur.execute(q, tuple(p)); rows = cur.fetchall()
    except: rows = []
    cur.close(); release_db(conn)
    return jsonify([r[0] for r in rows])


@app.route('/daily-active/chart-data')
def da_chart_data():
    if 'logged_user' not in session: return jsonify({'dates': [], 'full_dates': [], 'kccl_a': [], 'kccl_d': [], 'arohon_a': [], 'arohon_d': []})
    area = request.args.get('area', ''); sub_dist = request.args.get('sub_dist', ''); dist = request.args.get('distributor', '')
    conn = get_db()
    if not conn: return jsonify({'dates': [], 'full_dates': [], 'kccl_a': [], 'kccl_d': [], 'arohon_a': [], 'arohon_d': []})
    cur = conn.cursor()
    try:
        q = "SELECT d.report_date, SUM(d.active_count) as total_a, SUM(d.deactive_count) as total_d FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code = m.lco_code WHERE 1=1"
        params = []
        if area: q += " AND m.area = %s"; params.append(area)
        if sub_dist: q += " AND m.sub_distributor = %s"; params.append(sub_dist)
        q += " GROUP BY d.report_date ORDER BY d.report_date ASC"
        cur.execute(q, tuple(params))
        rows = cur.fetchall()
        arohon_map = {}
        if dist and dist.upper() == 'AROHON':
            try:
                q2 = "SELECT d.report_date, SUM(d.active_count) as aa, SUM(d.deactive_count) as ad FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code = m.lco_code WHERE COALESCE(m.distributor,'') = %s"
                params2 = [dist]
                if area: params2.append(area); q2 += " AND m.area = %s"
                if sub_dist: params2.append(sub_dist); q2 += " AND m.sub_distributor = %s"
                q2 += " GROUP BY d.report_date"
                cur2 = conn.cursor(); cur2.execute(q2, tuple(params2))
                for r in cur2.fetchall(): arohon_map[r[0].strftime('%Y-%m-%d')] = (int(r[1] or 0), int(r[2] or 0))
                cur2.close()
            except: pass
        # ★ FIX 1: Removed extra [] which caused ValueError
        kccl_a, kccl_d, arohon_a, arohon_d = [], [], [], [] 
        for r in rows:
            dt = r[0].strftime('%Y-%m-%d')
            aa, ad = arohon_map.get(dt, (0, 0))
            kccl_a.append(int(r[1] or 0) - aa)
            kccl_d.append(int(r[2] or 0) - ad)
            arohon_a.append(aa)
            arohon_d.append(ad)
        cur.close(); release_db(conn)
        return jsonify({'dates': [r[0].strftime('%d-%b') for r in rows], 'full_dates': [r[0].strftime('%Y-%m-%d') for r in rows], 'kccl_a': kccl_a, 'kccl_d': kccl_d, 'arohon_a': arohon_a, 'arohon_d': arohon_d})
    except Exception as e:
        print('[chart-data ERR]', e)
        try: cur.close()
        except: pass
        release_db(conn)
        return jsonify({'dates': [], 'full_dates': [], 'kccl_a': [], 'kccl_d': [], 'arohon_a': [], 'arohon_d': []})


@app.route('/daily-active/compare')
def da_compare():
    if 'logged_user' not in session: return jsonify({'error': 'Not logged in'})
    d_from = request.args.get('from', ''); d_to = request.args.get('to', '')
    area = request.args.get('area', ''); sub_dist = request.args.get('sub_dist', '')
    dist = request.args.get('distributor', ''); mode = request.args.get('mode', 'active')
    if not d_from or not d_to: return jsonify({'error': 'Select both dates'})
    if d_from == d_to: return jsonify({'error': 'Select two different dates'})
    conn = get_db()
    if not conn: return jsonify({'error': 'DB error'})
    cur = conn.cursor()
    try:
        # ★ FIX 2: Index 4 replaced '' with COALESCE(lm.distributor,'')
        q = """SELECT COALESCE(t1.lco_code,t2.lco_code), COALESCE(lm.lco_name,COALESCE(t1.lco_code,t2.lco_code)),
               COALESCE(lm.area,''), COALESCE(lm.sub_distributor,''), COALESCE(lm.distributor,''),
               COALESCE(t1.active_count,0), COALESCE(t2.active_count,0),
               COALESCE(t1.deactive_count,0), COALESCE(t2.deactive_count,0)
               FROM (SELECT lco_code,active_count,deactive_count FROM daily_active_summary WHERE report_date=%s) t1
               FULL OUTER JOIN (SELECT lco_code,active_count,deactive_count FROM daily_active_summary WHERE report_date=%s) t2
               ON t1.lco_code=t2.lco_code
               LEFT JOIN lco_master lm ON COALESCE(t1.lco_code,t2.lco_code)=lm.lco_code"""
        p = [d_from, d_to]
        if area or sub_dist:
            cs = []
            if area: cs.append("lm.area=%s"); p.append(area)
            if sub_dist: cs.append("lm.sub_distributor=%s"); p.append(sub_dist)
            if cs: q += " WHERE " + " AND ".join(cs)
        cur.execute(q, tuple(p))
        rows = cur.fetchall()
        
        # ★ FIX 3: Mapped correct indexes (6 is To-Date Active, 8 is To-Date Deactive)
        total_active = sum(r[6] for r in rows)
        total_deactive = sum(r[8] for r in rows)
        ka, kd, aa, ad = total_active, total_deactive, 0, 0
        
        if dist and dist.upper() == 'AROHON':
            try:
                q2 = """SELECT COALESCE(t1.active_count,0), COALESCE(t2.active_count,0), COALESCE(t1.deactive_count,0), COALESCE(t2.deactive_count,0)
                       FROM (SELECT lco_code,active_count,deactive_count FROM daily_active_summary WHERE report_date=%s) t1
                       FULL OUTER JOIN (SELECT lco_code,active_count,deactive_count FROM daily_active_summary WHERE report_date=%s) t2
                       ON t1.lco_code=t2.lco_code
                       LEFT JOIN lco_master lm ON COALESCE(t1.lco_code,t2.lco_code)=lm.lco_code
                       WHERE COALESCE(lm.distributor,'')=%s"""
                p2 = [d_from, d_to, dist]
                if area: p2.append(area); q2 += " AND lm.area=%s"
                if sub_dist: p2.append(sub_dist); q2 += " AND lm.sub_distributor=%s"
                cur2 = conn.cursor(); cur2.execute(q2, tuple(p2))
                for r in cur2.fetchall():
                    aa += r[1] # t2.active
                    ad += r[3] # t2.deactive
                cur2.close()
            except: pass
        ka = total_active - aa; kd = total_deactive - ad
        growth, churn = [], []; tg, tc = 0, 0
        for r in rows:
            if mode == 'active': change = r[6] - r[5]; prev_v, now_v = r[5], r[6]
            else: change = r[7] - r[8]; prev_v, now_v = r[7], r[8]
            entry = {'lco': r[0], 'name': r[1], 'area': r[2], 'sub': r[3], 'dist': r[4], 'prev': prev_v, 'now': now_v, 'change': change}
            if change > 0: growth.append(entry); tg += change
            elif change < 0: churn.append(entry); tc += change
        growth.sort(key=lambda x: x['change'], reverse=True); churn.sort(key=lambda x: x['change'])
        cur.close(); release_db(conn)
        return jsonify({'kccl_active': ka, 'kccl_deactive': kd, 'arohon_active': aa, 'arohon_deactive': ad, 'total_active': total_active, 'total_deactive': total_deactive, 'total_growth': tg, 'total_churn': abs(tc), 'net': tg + tc, 'growth': growth, 'churn': churn, 'd_from': d_from, 'd_to': d_to, 'mode': mode})
    except Exception as e:
        print('[compare ERR]', e)
        try: cur.close()
        except: pass
        release_db(conn)
        return jsonify({'error': 'Query failed: ' + str(e)})


@app.route('/daily-active/summary-tables')
def da_summary_tables():
    if 'logged_user' not in session: return jsonify({'areas': [], 'subs': []})
    d_from = request.args.get('from', '')
    d_to = request.args.get('to', '')
    area = request.args.get('area', '')
    sub_dist = request.args.get('sub_dist', '')
    dist = request.args.get('distributor', '')
    mode = request.args.get('mode', 'active')
    if not d_from or not d_to: return jsonify({'areas': [], 'subs': []})
    conn = get_db()
    if not conn: return jsonify({'areas': [], 'subs': []})
    cur = conn.cursor()
    try:
        fc = []
        fp = []
        if area: fc.append("m.area = %s"); fp.append(area)
        if sub_dist: fc.append("m.sub_distributor = %s"); fp.append(sub_dist)
        if dist: fc.append("COALESCE(m.distributor,'') = %s"); fp.append(dist)
        fsql = (" AND " + " AND ".join(fc)) if fc else ""

        a_q = f"""SELECT COALESCE(t1.name,t2.name),
                   COALESCE(t1.act,0),COALESCE(t2.act,0),COALESCE(t2.act,0)-COALESCE(t1.act,0),
                   COALESCE(t1.deact,0),COALESCE(t2.deact,0),COALESCE(t2.deact,0)-COALESCE(t1.deact,0),
                   COALESCE(t2.lcos,0)
                   FROM (SELECT COALESCE(m.area,'Unassigned') as name,SUM(d.active_count) as act,SUM(d.deactive_count) as deact,COUNT(DISTINCT d.lco_code) as lcos
                         FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code=m.lco_code WHERE d.report_date=%s{fsql}
                         GROUP BY COALESCE(m.area,'Unassigned')) t1
                   FULL OUTER JOIN (SELECT COALESCE(m.area,'Unassigned') as name,SUM(d.active_count) as act,SUM(d.deactive_count) as deact,COUNT(DISTINCT d.lco_code) as lcos
                         FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code=m.lco_code WHERE d.report_date=%s{fsql}
                         GROUP BY COALESCE(m.area,'Unassigned')) t2 ON t1.name=t2.name
                   ORDER BY (COALESCE(t2.act,0)-COALESCE(t1.act,0)) DESC"""
        s_q = f"""SELECT COALESCE(t1.name,t2.name),
                   COALESCE(t1.act,0),COALESCE(t2.act,0),COALESCE(t2.act,0)-COALESCE(t1.act,0),
                   COALESCE(t1.deact,0),COALESCE(t2.deact,0),COALESCE(t2.deact,0)-COALESCE(t1.deact,0),
                   COALESCE(t2.lcos,0)
                   FROM (SELECT COALESCE(m.sub_distributor,'Unassigned') as name,SUM(d.active_count) as act,SUM(d.deactive_count) as deact,COUNT(DISTINCT d.lco_code) as lcos
                         FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code=m.lco_code WHERE d.report_date=%s{fsql}
                         GROUP BY COALESCE(m.sub_distributor,'Unassigned')) t1
                   FULL OUTER JOIN (SELECT COALESCE(m.sub_distributor,'Unassigned') as name,SUM(d.active_count) as act,SUM(d.deactive_count) as deact,COUNT(DISTINCT d.lco_code) as lcos
                         FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code=m.lco_code WHERE d.report_date=%s{fsql}
                         GROUP BY COALESCE(m.sub_distributor,'Unassigned')) t2 ON t1.name=t2.name
                   ORDER BY (COALESCE(t2.act,0)-COALESCE(t1.act,0)) DESC"""

        ap = [d_from]+fp+[d_to]+fp
        sp = [d_from]+fp+[d_to]+fp
        cur.execute(a_q, ap); area_rows = cur.fetchall()
        cur.execute(s_q, sp); sub_rows = cur.fetchall()
        cur.close(); release_db(conn)

        def fmt(rows, mv):
            res = []
            for r in rows:
                if mv == 'active':
                    res.append({'name':r[0],'prev':int(r[1] or 0),'now':int(r[2] or 0),'change':int(r[3] or 0),'lcos':int(r[7] or 0)})
                else:
                    res.append({'name':r[0],'prev':int(r[4] or 0),'now':int(r[5] or 0),'change':int(r[6] or 0),'lcos':int(r[7] or 0)})
            res.sort(key=lambda x: x['change'], reverse=True)
            return res
        return jsonify({'areas': fmt(area_rows, mode), 'subs': fmt(sub_rows, mode)})
    except Exception as e:
        print('[summary ERR]', e)
        try: cur.close()
        except: pass
        release_db(conn)
        return jsonify({'areas': [], 'subs': []})

@app.route('/daily-active/date-summary')
def da_date_summary():
    if 'logged_user' not in session: return redirect(url_for('login'))
    d = request.args.get('date', '')
    if not d: return redirect(url_for('daily_active'))
    conn = get_db()
    if not conn: return redirect(url_for('daily_active'))
    try:
        df = pd.read_sql("SELECT d.lco_code as \"LCO Code\", COALESCE(m.lco_name,d.lco_code) as \"LCO Name\", COALESCE(m.area,'') as \"Area\", COALESCE(m.sub_distributor,'') as \"Sub Distributor\", COALESCE(m.distributor,'') as \"Distributor\", d.active_count as \"Active\", d.deactive_count as \"Deactive\" FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code = m.lco_code WHERE d.report_date = %s ORDER BY d.active_count DESC", conn, params=(d,))
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, f"Summary_{d}.xlsx")
    except Exception as e:
        release_db(conn); flash(f"Export Error: {e}", "error")
        return redirect(url_for('daily_active'))


@app.route('/daily-active/export-growth')
def export_growth_report():
    if 'logged_user' not in session: return redirect(url_for('login'))
    data, err = _export_compare_base(request.args.get('from', ''), request.args.get('to', ''), request.args.get('mode', 'active'), request.args.get('area', ''), request.args.get('sub_dist', ''), request.args.get('distributor', ''), True)
    if err: flash(f"Error: {err}", "error"); return redirect(url_for('daily_active'))
    if data is None: flash('No growth data', 'error'); return redirect(url_for('daily_active'))
    return dl_excel(data, f"Growth_Report_{request.args.get('from','')}.xlsx")

@app.route('/daily-active/export-churn')
def export_churn_report():
    if 'logged_user' not in session: return redirect(url_for('login'))
    data, err = _export_compare_base(request.args.get('from', ''), request.args.get('to', ''), request.args.get('mode', 'active'), request.args.get('area', ''), request.args.get('sub_dist', ''), request.args.get('distributor', ''), False)
    if err: flash(f"Error: {err}", "error"); return redirect(url_for('daily_active'))
    if data is None: flash('No churn data', 'error'); return redirect(url_for('daily_active'))
    return dl_excel(data, f"Churn_Report_{request.args.get('from','')}.xlsx")


@app.route('/daily-active/export')
def export_daily_active():
    if 'logged_user' not in session: return redirect(url_for('login'))
    conn = get_db()
    if not conn: return redirect(url_for('dashboard'))
    try:
        df = pd.read_sql("SELECT d.report_date as \"Report Date\", d.lco_code as \"LCO Code\", COALESCE(m.lco_name,'') as \"LCO Name\", COALESCE(m.area,'') as \"Area\", COALESCE(m.sub_distributor,'') as \"Sub Distributor\", d.active_count as \"Active\", d.deactive_count as \"Deactive\", d.created_at as \"Uploaded At\" FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code=m.lco_code ORDER BY d.report_date DESC, d.lco_code ASC", conn)
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, "Daily_Active_Full.xlsx")
    except Exception as e:
        flash(f"Export Error: {e}", "error"); release_db(conn)
        return redirect(url_for('daily_active'))


@app.route('/daily-active/template')
def daily_active_template():
    if 'logged_user' not in session: return redirect(url_for('login'))
    output = BytesIO()
    df = pd.DataFrame(columns=['report_date', 'lco_name', 'active_count', 'deactive_count', 'distributor'])
    df.loc[0] = ['2026-04-01', 'ANIMA CABLE NETWORK', 540, 12, 'KCCL']
    df.loc[1] = ['2026-04-01', 'BABLU CABLE', 421, 5, 'AROHON']
    with pd.ExcelWriter(output, engine='openpyxl') as writer: df.to_excel(writer, sheet_name="Template", index=False)
    output.seek(0)
    return send_file(output, download_name="Daily_Active_Template.xlsx", as_attachment=True)


@app.route('/daily-active/delete-all', methods=['POST'])
def daily_active_delete_all():
    if 'logged_user' not in session or session.get('user_role') != 'admin': return redirect(url_for('login'))
    conn = get_db()
    if not conn: flash('Database connection failed', 'error'); return redirect(url_for('daily_active'))
    try:
        cur = conn.cursor(); cur.execute("DELETE FROM daily_active_summary"); conn.commit()
        flash('All records deleted', 'success'); cur.close()
    except Exception as e:
        conn.rollback(); flash(f'Error: {e}', 'error')
    release_db(conn)
    return redirect(url_for('daily_active'))
    
if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
