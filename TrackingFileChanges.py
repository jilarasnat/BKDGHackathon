import os
import sqlite3
import hashlib
import datetime

def getmodtime(fname):
    """Get file modified time"""
    try:
        mtime = os.path.getmtime(fname)
    except OSError as emsg:
        print(str(emsg))
        mtime = 0
    return mtime

def md5short(fname):
    """Get md5 file hast tag..."""
    enc = fname + '|' + str(getmodtime(fname))
    return hashlib.md5(enc.encode('utf-8')).hexdigest()

def getbasefile():
    """Name of the SQLite DB file"""
    return os.path.splitext(os.path.basename(__file__))[0]

def connectdb():
    """Connect to the SQLite DB"""
    try:
        dbfile = getbasefile() + '.db'
        conn = sqlite3.connect(dbfile, timeout=2)
    except BaseException as err:
        print(str(err))
        conn = None
    return conn

def corecursor(conn, query):
    """Opens a SQLite DB cursor"""
    result = False
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        numrows = len(list(rows))
        if numrows > 0:
            result = True
    except sqlite3.OperationalError as err:
        print(str(err))
        cursor.close()
    finally:
        cursor.close()
    return result

def tableexists(table):
    """Checks if a SQLite DB Table exists"""
    result = False
    core = "SELECT name FROM sqlite_master WHERE type='table' AND name='"
    try:
        conn = connectdb()
        if not conn is None:
            query = core + table + "'"
            result = corecursor(conn, query)
            conn.close()
    except sqlite3.OperationalError as err:
        print(str(err))
        conn.close()
    return result

def createhashtableidx():
    """Creates a SQLite DB Table Index"""
    table = 'files'
    query = 'CREATE INDEX idxfile ON files (file, md5)'
    try:
        conn = connectdb()
        if not conn is None:
            if not tableexists(table):
                try:
                    cursor = conn.cursor()
                    cursor.execute(query)
                except sqlite3.OperationalError:
                    cursor.close()
                finally:
                    conn.commit()
                    cursor.close()
    except sqlite3.OperationalError as err:
        print(str(err))
        conn.close()
    finally:
        conn.close()

def createhashtable():
    """Creates a SQLite DB Table"""
    result = False
    query = "CREATE TABLE files ({file} {ft} PRIMARY KEY, {md5} {ft})"\
        .format(file='file', md5='md5', ft='TEXT')
    try:
        conn = connectdb()
        if not conn is None:
            if not tableexists('files'):
                try:
                    cursor = conn.cursor()
                    cursor.execute(query)
                except sqlite3.OperationalError:
                    cursor.close()
                finally:
                    conn.commit()
                    cursor.close()
                    result = True
    except sqlite3.OperationalError as err:
        print(str(err))
        conn.close()
    finally:
        conn.close()
    return result

def runcmd(qry):
    """Run a specific command on the SQLite DB"""
    try:
        conn = connectdb()
        if not conn is None:
            if tableexists('files'):
                try:
                    cursor = conn.cursor()
                    cursor.execute(qry)
                except sqlite3.OperationalError:
                    cursor.close()
                finally:
                    conn.commit()
                    cursor.close()
    except sqlite3.OperationalError as err:
        print(str(err))
        conn.close()
    finally:
        conn.close()

def updatehashtable(fname, md5):
    """Update the SQLite File Table"""
    qry = "UPDATE files SET md5='{md5}' WHERE file='{fname}'"\
        .format(fname=fname, md5=md5)
    runcmd(qry)

def inserthashtable(fname, md5):
    """Insert into the SQLite File Table"""
    qry = "INSERT INTO files (file, md5) VALUES ('{fname}', '{md5}')"\
        .format(fname=fname, md5=md5)
    runcmd(qry)

def setuphashtable(fname, md5):
    """Sets Up the Hash Table"""
    createhashtable()
    createhashtableidx()
    inserthashtable(fname, md5)

def md5indb(fname):
    """Checks if md5 hash tag exists in the SQLite DB"""
    items = []
    qry = "SELECT md5 FROM files WHERE file = '" + fname + "'"
    try:
        conn = connectdb()
        if not conn is None:
            if tableexists('files'):
                try:
                    cursor = conn.cursor()
                    cursor.execute(qry)
                    for row in cursor:
                        items.append(row[0])
                except sqlite3.OperationalError as err:
                    print(str(err))
                    cursor.close()
                finally:
                    cursor.close()
    except sqlite3.OperationalError as err:
        print(str(err))
        conn.close()
    finally:
        conn.close()
    return items

def haschanged(fname, md5):
    """Checks if a file has changed"""
    result = False
    oldmd5 = md5indb(fname)
    numits = len(oldmd5)
    if numits > 0:
        if oldmd5[0] != md5:
            result = True
            updatehashtable(fname, md5)
    else:
        setuphashtable(fname, md5)
    return result

def getfileext(fname):
    """Get the file name extension"""
    return os.path.splitext(fname)[1]

def checkfilechanges(folder, exclude):
    """Checks for files changes"""
    result = []
    mtime = 0
    for subdir, dirs, files in os.walk(folder):
        for fname in files:
            origin = os.path.join(subdir, fname)
            if os.path.isfile(origin):
                fext = getfileext(origin)
                if not fext in exclude:
                    md5 = md5short(origin)
                    if haschanged(origin, md5):
                        mtime = datetime.datetime.fromtimestamp(getmodtime(origin))
                        result.append(origin)
                        result.append(' has changed...')
                        result.append(str(mtime))
                    else:
                        mtime = datetime.datetime.fromtimestamp(getmodtime(origin))
                        result.append(origin)
                        result.append(' has NOT changed...')
                        result.append(str(mtime))
    return result