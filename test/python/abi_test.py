import duckdb

def test_abi():
    conn = duckdb.connect('');
    conn.execute("SELECT abi('Sam') as value;");
    res = conn.fetchall()
    assert(res[0][0] == "Abi Sam 🐥");