"""Real pg_dump/pg_restore rehearsal on explicitly local synthetic databases only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid

import psycopg
from psycopg import sql


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--pg-bin',type=Path,required=True)
    parser.add_argument('--proof',type=Path,required=True)
    args=parser.parse_args()
    args.proof.mkdir(parents=True,exist_ok=True)
    safe={k:os.environ[k] for k in ['SYSTEMROOT','PATH','TEMP','TMP','LANG'] if k in os.environ}
    safe.update(PGHOST='127.0.0.1',PGPORT='55439',PGUSER='audit',PGPASSFILE=str(args.proof/'nonexistent-passfile'),PGCONNECT_TIMEOUT='3')
    connection=dict(host='127.0.0.1',port=55439,user='audit',dbname='vydno_audit',password='',passfile=str(args.proof/'nonexistent-passfile'),connect_timeout=3)
    prefix='vydno_ops_backup_'+uuid.uuid4().hex[:12]
    names=[prefix+'_source',prefix+'_restore']
    proof={'scope':'local synthetic PostgreSQL only','production_backup_verified':False,'databases':names}
    start=time.monotonic()
    with psycopg.connect(**connection,autocommit=True) as admin:
        identity=admin.execute("SELECT current_database(), current_user, host(inet_server_addr()), current_setting('listen_addresses'), current_setting('data_directory')").fetchone()
        assert identity[:3]==('vydno_audit','audit','127.0.0.1'),identity[:3]
        assert identity[3]=='127.0.0.1'
        assert '/temp/vydno-' in identity[4].replace('\\','/').lower()
        proof['server_version']=admin.execute('SELECT version()').fetchone()[0]
        created=[]
        try:
            for name in names:
                admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)));created.append(name)
            with psycopg.connect(**{**connection,'dbname':names[0]}) as db:
                db.execute('CREATE TABLE synthetic_accounts(id integer PRIMARY KEY, balance numeric(18,2) NOT NULL)')
                db.execute('CREATE TABLE synthetic_ledger(id integer PRIMARY KEY, account_id integer REFERENCES synthetic_accounts(id), amount numeric(18,2) NOT NULL)')
                db.execute('CREATE TABLE synthetic_receipts(id integer PRIMARY KEY, ledger_id integer UNIQUE REFERENCES synthetic_ledger(id))')
                db.execute("CREATE TABLE synthetic_billing(id integer PRIMARY KEY, invoice text UNIQUE, amount numeric(18,2) CHECK(amount > 0), status text CHECK(status IN ('paid','pending')))")
                db.execute('INSERT INTO synthetic_accounts VALUES (1,875.50)')
                db.execute('INSERT INTO synthetic_ledger VALUES (1,1,1000.00),(2,1,-124.50)')
                db.execute('INSERT INTO synthetic_receipts VALUES (1,1),(2,2)')
                db.execute("INSERT INTO synthetic_billing VALUES (1,'synthetic-invoice',499.00,'paid')")
            dump=args.proof/'synthetic-backup.dump'
            ext='.exe' if os.name=='nt' else ''
            subprocess.run([str(args.pg_bin/('pg_dump'+ext)),'--no-owner','--no-privileges','--format=custom','--file',str(dump),'--dbname',names[0]],env=safe,check=True,capture_output=True,timeout=30)
            dumped_at=time.monotonic()
            subprocess.run([str(args.pg_bin/('pg_restore'+ext)),'--exit-on-error','--single-transaction','--no-owner','--no-privileges','--dbname',names[1],str(dump)],env=safe,check=True,capture_output=True,timeout=30)
            snapshots=[]
            for name in names:
                with psycopg.connect(**{**connection,'dbname':name}) as db:
                    counts={t:db.execute(sql.SQL('SELECT count(*) FROM {}').format(sql.Identifier(t))).fetchone()[0] for t in ['synthetic_accounts','synthetic_ledger','synthetic_receipts','synthetic_billing']}
                    balanced=db.execute('SELECT a.balance=SUM(l.amount) FROM synthetic_accounts a JOIN synthetic_ledger l ON l.account_id=a.id GROUP BY a.id').fetchall()
                    assert balanced==[(True,)]
                    snapshots.append(counts)
                    try:
                        with db.transaction(): db.execute('INSERT INTO synthetic_receipts VALUES (3,1)')
                    except psycopg.errors.UniqueViolation:
                        pass
                    else: raise AssertionError('Receipt uniqueness not restored')
                    try:
                        with db.transaction(): db.execute('INSERT INTO synthetic_ledger VALUES (3,99,1.00)')
                    except psycopg.errors.ForeignKeyViolation:
                        pass
                    else: raise AssertionError('Ledger FK not restored')
            assert snapshots[0]==snapshots[1]
            proof.update(verified=True,row_counts=snapshots[1],ledger_balanced=True,receipt_unique=True,ledger_foreign_key=True,restore_seconds=round(time.monotonic()-dumped_at,3),backup_sha256=hashlib.sha256(dump.read_bytes()).hexdigest(),elapsed_seconds=round(time.monotonic()-start,3))
        finally:
            for name in reversed(created):
                admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(name)))
            assert not admin.execute('SELECT datname FROM pg_database WHERE datname=ANY(%s)',[names]).fetchall()
            proof['synthetic_databases_removed']=True
            (args.proof/'restore-proof.json').write_text(json.dumps(proof,indent=2),encoding='utf-8')
    print(json.dumps(proof,indent=2))


if __name__=='__main__': main()
