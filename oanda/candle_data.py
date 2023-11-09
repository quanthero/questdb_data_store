import psycopg2
from oandapyV20 import API
import oandapyV20.endpoints.instruments as instruments

# Replace with your actual new access token and account ID
access_token = "***"
accountID = "***"
client = API(access_token=access_token, environment='live')

instrument = "EUR_USD"
params = {
    "from": "2023-01-02T12:43:00.000000Z",
    "to" : "2023-01-05T00:00:00Z",
    "granularity": "M1"
    # "count": 500,
}

r = instruments.InstrumentsCandles(instrument=instrument, params=params)

try:
    # Send the request to OANDA
    response = client.request(r)
    candles = response.get('candles')

    # Connect to QuestDB
    conn = psycopg2.connect(
        dbname='qdb',
        user='admin',
        password='quest',
        host='208.115.218.94',
        port='28812'
    )
    cur = conn.cursor()

    # Prepare SQL insert statement
    insert_stmt = """
    INSERT INTO oanda_candlestick_data (Time, Open, High, Low, Close, Volume) VALUES (%s, %s, %s, %s, %s, %s);
    """

    # Insert candlestick data into QuestDB
    for candle in candles:
        time = candle['time']
        volume = candle['volume']
        mid = candle['mid']
        o = mid['o']  # Open price
        h = mid['h']  # High price
        l = mid['l']  # Low price
        c = mid['c']  # Close price

        # Execute the insert statement
        cur.execute(insert_stmt, (time, o, h, l, c, volume))

    # Commit the transaction
    conn.commit()

    # Close the cursor and the connection
    cur.close()
    conn.close()

    print("Data ingested to QuestDB")

except Exception as e:
    print("An error occurred: {}".format(e))
