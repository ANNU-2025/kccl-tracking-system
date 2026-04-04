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
        wheres = []
        if area: wheres.append("lm.area=%s"); p.append(area)
        if sub_dist: wheres.append("lm.sub_distributor=%s"); p.append(sub_dist)
        if dist: wheres.append("COALESCE(lm.distributor,'')=%s"); p.append(dist)
        if wheres: q += " WHERE " + " AND ".join(wheres)
        cur.execute(q, tuple(p))
        rows = cur.fetchall()
        cur.close()
        release_db(conn)
        data = []
        for r in rows:
            if mode == 'active': change = r[6] - r[5]; prev_v, now_v = r[5], r[6]
            else: change = r[7] - r[8]; prev_v, now_v = r[7], r[8]
            if is_growth and change > 0: data.append({'LCO Code': r[0], 'LCO Name': r[1], 'Area': r[2], 'Sub Dist': r[3], 'Distributor': r[4] or '', 'Prev': prev_v, 'Now': now_v, 'Change': change})
            elif not is_growth and change < 0: data.append({'LCO Code': r[0], 'LCO Name': r[1], 'Area': r[2], 'Sub Dist': r[3], 'Distributor': r[4] or '', 'Prev': prev_v, 'Now': now_v, 'Change': change})
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
    if 'logged_user' not in session: return jsonify({'dates':[],'full_dates':[],'kccl_a':[],'kccl_d':[],'arohon_a':[],'arohon_d':[]})
    conn = get_db()
    if not conn: return jsonify({'dates':[],'full_dates':[],'kccl_a':[],'kccl_d':[],'arohon_a':[],'arohon_d':[]})
    cur = conn.cursor()
    try:
        cur.execute("SELECT report_date FROM daily_active_summary ORDER BY report_date DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            cur.close(); release_db(conn)
            return jsonify({'dates':[],'full_dates':[],'kccl_a':[],'kccl_d':[],'arohon_a':[],'arohon_d':[]})
        latest = row[0]
        cur.execute("""SELECT
            SUM(CASE WHEN COALESCE(m.distributor,'')!='AROHON' THEN d.active_count ELSE 0 END),
            SUM(CASE WHEN COALESCE(m.distributor,'')='AROHON' THEN d.active_count ELSE 0 END),
            SUM(CASE WHEN COALESCE(m.distributor,'')!='AROHON' THEN d.deactive_count ELSE 0 END),
            SUM(CASE WHEN COALESCE(m.distributor,'')='AROHON' THEN d.deactive_count ELSE 0 END)
            FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code = m.lco_code
            WHERE d.report_date=%s""", (latest,))
        r = cur.fetchone()
        ka = int(r[0] or 0); aa = int(r[1] or 0)
        kd = int(r[2] or 0); ad = int(r[3] or 0)
        label = latest.strftime('%d-%b')
        full = latest.strftime('%Y-%m-%d')
        cur.close(); release_db(conn)
        return jsonify({'dates':[label],'full_dates':[full],'kccl_a':[ka],'kccl_d':[kd],'arohon_a':[aa],'arohon_d':[ad]})
    except Exception as e:
        print('[chart-data ERR]',e)
        try: cur.close()
        except: pass
        release_db(conn)
        return jsonify({'dates':[],'full_dates':[],'kccl_a':[],'kccl_d':[],'arohon_a':[],'arohon_d':[]})


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
        q = """SELECT COALESCE(t1.lco_code,t2.lco_code), COALESCE(lm.lco_name,COALESCE(t1.lco_code,t2.lco_code)),
               COALESCE(lm.area,''), COALESCE(lm.sub_distributor,''), COALESCE(lm.distributor,''),
               COALESCE(t1.active_count,0), COALESCE(t2.active_count,0),
               COALESCE(t1.deactive_count,0), COALESCE(t2.deactive_count,0)
               FROM (SELECT lco_code,active_count,deactive_count FROM daily_active_summary WHERE report_date=%s) t1
               FULL OUTER JOIN (SELECT lco_code,active_count,deactive_count FROM daily_active_summary WHERE report_date=%s) t2
               ON t1.lco_code=t2.lco_code
               LEFT JOIN lco_master lm ON COALESCE(t1.lco_code,t2.lco_code)=lm.lco_code"""
        p = [d_from, d_to]
        wheres = []
        if area: wheres.append("lm.area=%s"); p.append(area)
        if sub_dist: wheres.append("lm.sub_distributor=%s"); p.append(sub_dist)
        if dist: wheres.append("COALESCE(lm.distributor,'')=%s"); p.append(dist)
        if wheres: q += " WHERE " + " AND ".join(wheres)
        cur.execute(q, tuple(p))
        rows = cur.fetchall()

        total_active = sum(r[6] for r in rows)
        total_deactive = sum(r[8] for r in rows)
        aa = sum(r[6] for r in rows if (r[4] or '').upper() == 'AROHON')
        ad = sum(r[8] for r in rows if (r[4] or '').upper() == 'AROHON')
        ka = total_active - aa
        kd = total_deactive - ad

        growth, churn = [], []; tg, tc = 0, 0
        for r in rows:
            if mode == 'active': change = r[6] - r[5]; prev_v, now_v = r[5], r[6]
            else: change = r[7] - r[8]; prev_v, now_v = r[7], r[8]
            entry = {'lco': r[0], 'name': r[1], 'area': r[2], 'sub': r[3], 'dist': r[4] or 'KCCL', 'prev': prev_v, 'now': now_v, 'change': change}
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
    d_from = request.args.get('from', ''); d_to = request.args.get('to', '')
    area = request.args.get('area', ''); sub_dist = request.args.get('sub_dist', '')
    dist = request.args.get('distributor', ''); mode = request.args.get('mode', 'active')
    if not d_from or not d_to: return jsonify({'areas': [], 'subs': []})
    conn = get_db()
    if not conn: return jsonify({'areas': [], 'subs': []})
    cur = conn.cursor()
    try:
        fc = []; fp = []
        if area: fc.append("m.area = %s"); fp.append(area)
        if sub_dist: fc.append("m.sub_distributor = %s"); fp.append(sub_dist)
        if dist: fc.append("COALESCE(m.distributor,'') = %s"); fp.append(dist)
        fsql = (" AND " + " AND ".join(fc)) if fc else ""

        def build_q(group_col):
            col_expr = f"COALESCE(m.{group_col},'Unassigned')"
            inner = f"""SELECT {col_expr} as name,
                SUM(CASE WHEN COALESCE(m.distributor,'')!='AROHON' THEN d.active_count ELSE 0 END) as ka,
                SUM(CASE WHEN COALESCE(m.distributor,'')='AROHON' THEN d.active_count ELSE 0 END) as aa,
                SUM(CASE WHEN COALESCE(m.distributor,'')!='AROHON' THEN d.deactive_count ELSE 0 END) as kd,
                SUM(CASE WHEN COALESCE(m.distributor,'')='AROHON' THEN d.deactive_count ELSE 0 END) as ad,
                COUNT(DISTINCT d.lco_code) as lcos
                FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code=m.lco_code
                WHERE d.report_date=%s{fsql} GROUP BY {col_expr}"""
            return f"""SELECT COALESCE(t1.name,t2.name),
                COALESCE(t1.ka,0),COALESCE(t2.ka,0),
                COALESCE(t1.aa,0),COALESCE(t2.aa,0),
                COALESCE(t1.kd,0),COALESCE(t2.kd,0),
                COALESCE(t1.ad,0),COALESCE(t2.ad,0),
                COALESCE(t2.lcos,0)
                FROM ({inner}) t1 FULL OUTER JOIN ({inner}) t2 ON t1.name=t2.name"""

        a_q = build_q('area')
        s_q = build_q('sub_distributor')
        ap = [d_from]+fp+[d_to]+fp
        sp = [d_from]+fp+[d_to]+fp
        cur.execute(a_q, ap); area_rows = cur.fetchall()
        cur.execute(s_q, sp); sub_rows = cur.fetchall()
        cur.close(); release_db(conn)

        def fmt(rows, mv):
            res = []
            for r in rows:
                if mv == 'active':
                    kp, kn, ap2, an2 = int(r[1] or 0), int(r[2] or 0), int(r[3] or 0), int(r[4] or 0)
                else:
                    kp, kn, ap2, an2 = int(r[5] or 0), int(r[6] or 0), int(r[7] or 0), int(r[8] or 0)
                res.append({'name': r[0], 'kccl_prev': kp, 'kccl_now': kn, 'kccl_change': kn - kp,
                    'arohon_prev': ap2, 'arohon_now': an2, 'arohon_change': an2 - ap2,
                    'total_change': (kn - kp) + (an2 - ap2), 'lcos': int(r[9] or 0)})
            res.sort(key=lambda x: x['total_change'], reverse=True)
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


@app.route('/daily-active/export-subdist')
def export_subdist_summary():
    if 'logged_user' not in session: return redirect(url_for('login'))
    d_from = request.args.get('from', ''); d_to = request.args.get('to', '')
    area = request.args.get('area', ''); sub_dist = request.args.get('sub_dist', '')
    dist = request.args.get('distributor', ''); mode = request.args.get('mode', 'active')
    if not d_from or not d_to: flash('Select dates', 'error'); return redirect(url_for('daily_active'))
    conn = get_db()
    if not conn: flash('DB error', 'error'); return redirect(url_for('daily_active'))
    try:
        fc = []; fp = []
        if area: fc.append("m.area = %s"); fp.append(area)
        if sub_dist: fc.append("m.sub_distributor = %s"); fp.append(sub_dist)
        if dist: fc.append("COALESCE(m.distributor,'') = %s"); fp.append(dist)
        fsql = (" AND " + " AND ".join(fc)) if fc else ""
        col_expr = "COALESCE(m.sub_distributor,'Unassigned')"
        inner = f"""SELECT {col_expr} as name,
            SUM(CASE WHEN COALESCE(m.distributor,'')!='AROHON' THEN d.active_count ELSE 0 END) as ka,
            SUM(CASE WHEN COALESCE(m.distributor,'')='AROHON' THEN d.active_count ELSE 0 END) as aa,
            SUM(CASE WHEN COALESCE(m.distributor,'')!='AROHON' THEN d.deactive_count ELSE 0 END) as kd,
            SUM(CASE WHEN COALESCE(m.distributor,'')='AROHON' THEN d.deactive_count ELSE 0 END) as ad,
            COUNT(DISTINCT d.lco_code) as lcos
            FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code=m.lco_code
            WHERE d.report_date=%s{fsql} GROUP BY {col_expr}"""
        q = f"""SELECT COALESCE(t1.name,t2.name),
            COALESCE(t1.ka,0),COALESCE(t2.ka,0),COALESCE(t1.aa,0),COALESCE(t2.aa,0),
            COALESCE(t1.kd,0),COALESCE(t2.kd,0),COALESCE(t1.ad,0),COALESCE(t2.ad,0),
            COALESCE(t2.lcos,0)
            FROM ({inner}) t1 FULL OUTER JOIN ({inner}) t2 ON t1.name=t2.name"""
        params = [d_from]+fp+[d_to]+fp
        df = pd.read_sql(q, conn, params=params)
        if mode == 'active':
            df.columns = ['Sub Distributor', 'KCCL Prev', 'KCCL Now', 'AROHON Prev', 'AROHON Now', '_', '_', '_', '_', 'LCOs']
        else:
            df.columns = ['Sub Distributor', '_', '_', '_', '_', 'KCCL Prev', 'KCCL Now', 'AROHON Prev', 'AROHON Now', 'LCOs']
        df['KCCL Change'] = (df['KCCL Now'] - df['KCCL Prev']).fillna(0).astype(int)
        df['AROHON Change'] = (df['AROHON Now'] - df['AROHON Prev']).fillna(0).astype(int)
        df = df[['Sub Distributor', 'KCCL Prev', 'KCCL Now', 'KCCL Change', 'AROHON Prev', 'AROHON Now', 'AROHON Change', 'LCOs']]
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, f"SubDist_Summary_{d_from}_to_{d_to}.xlsx")
    except Exception as e:
        flash(f"Export Error: {e}", "error"); release_db(conn)
        return redirect(url_for('daily_active'))


@app.route('/daily-active/export-area')
def export_area_summary():
    if 'logged_user' not in session: return redirect(url_for('login'))
    d_from = request.args.get('from', ''); d_to = request.args.get('to', '')
    area = request.args.get('area', ''); sub_dist = request.args.get('sub_dist', '')
    dist = request.args.get('distributor', ''); mode = request.args.get('mode', 'active')
    if not d_from or not d_to: flash('Select dates', 'error'); return redirect(url_for('daily_active'))
    conn = get_db()
    if not conn: flash('DB error', 'error'); return redirect(url_for('daily_active'))
    try:
        fc = []; fp = []
        if area: fc.append("m.area = %s"); fp.append(area)
        if sub_dist: fc.append("m.sub_distributor = %s"); fp.append(sub_dist)
        if dist: fc.append("COALESCE(m.distributor,'') = %s"); fp.append(dist)
        fsql = (" AND " + " AND ".join(fc)) if fc else ""
        col_expr = "COALESCE(m.area,'Unassigned')"
        inner = f"""SELECT {col_expr} as name,
            SUM(CASE WHEN COALESCE(m.distributor,'')!='AROHON' THEN d.active_count ELSE 0 END) as ka,
            SUM(CASE WHEN COALESCE(m.distributor,'')='AROHON' THEN d.active_count ELSE 0 END) as aa,
            SUM(CASE WHEN COALESCE(m.distributor,'')!='AROHON' THEN d.deactive_count ELSE 0 END) as kd,
            SUM(CASE WHEN COALESCE(m.distributor,'')='AROHON' THEN d.deactive_count ELSE 0 END) as ad,
            COUNT(DISTINCT d.lco_code) as lcos
            FROM daily_active_summary d LEFT JOIN lco_master m ON d.lco_code=m.lco_code
            WHERE d.report_date=%s{fsql} GROUP BY {col_expr}"""
        q = f"""SELECT COALESCE(t1.name,t2.name),
            COALESCE(t1.ka,0),COALESCE(t2.ka,0),COALESCE(t1.aa,0),COALESCE(t2.aa,0),
            COALESCE(t1.kd,0),COALESCE(t2.kd,0),COALESCE(t1.ad,0),COALESCE(t2.ad,0),
            COALESCE(t2.lcos,0)
            FROM ({inner}) t1 FULL OUTER JOIN ({inner}) t2 ON t1.name=t2.name"""
        params = [d_from]+fp+[d_to]+fp
        df = pd.read_sql(q, conn, params=params)
        if mode == 'active':
            df.columns = ['Area', 'KCCL Prev', 'KCCL Now', 'AROHON Prev', 'AROHON Now', '_', '_', '_', '_', 'LCOs']
        else:
            df.columns = ['Area', '_', '_', '_', '_', 'KCCL Prev', 'KCCL Now', 'AROHON Prev', 'AROHON Now', 'LCOs']
        df['KCCL Change'] = (df['KCCL Now'] - df['KCCL Prev']).fillna(0).astype(int)
        df['AROHON Change'] = (df['AROHON Now'] - df['AROHON Prev']).fillna(0).astype(int)
        df = df[['Area', 'KCCL Prev', 'KCCL Now', 'KCCL Change', 'AROHON Prev', 'AROHON Now', 'AROHON Change', 'LCOs']]
        df = fix_timezone(df); release_db(conn)
        return dl_excel(df, f"Area_Summary_{d_from}_to_{d_to}.xlsx")
    except Exception as e:
        flash(f"Export Error: {e}", "error"); release_db(conn)
        return redirect(url_for('daily_active'))


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
