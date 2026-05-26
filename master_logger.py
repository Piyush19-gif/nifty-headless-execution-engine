import os, time, csv
from datetime import datetime, date
import requests, pyotp
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket.data_ws import FyersDataSocket

# ═══════════════════════════════════════════════════════════════
#  CREDENTIALS
# ═══════════════════════════════════════════════════════════════
APP_ID       = ""
SECRET_KEY   = ""
REDIRECT_URI = ""
FY_ID        = ""       
PIN          = ""           
TOTP_KEY     = ""       

# ═══════════════════════════════════════════════════════════════
#  LIQUIDITY WINDOW CONFIGURATION
# ═══════════════════════════════════════════════════════════════
EXPIRY         = "26MAY"
CENTRAL_STRIKE = 23350   # Update this to the current ATM before running
STEP           = 50      
DEPTH          = 10      

# Generate symbols: Nifty Spot + 20 CE + 20 PE
SPOT_SYMBOL = "NSE:NIFTY50-INDEX"
SYMBOLS = [SPOT_SYMBOL]
for i in range(-DEPTH, DEPTH + 1):
    strike = CENTRAL_STRIKE + (i * STEP)
    SYMBOLS.append(f"NSE:NIFTY{EXPIRY}{strike}CE")
    SYMBOLS.append(f"NSE:NIFTY{EXPIRY}{strike}PE")

print(f"Tracking {len(SYMBOLS)} symbols (1 Spot + {len(SYMBOLS)-1} Options)...")

# ═══════════════════════════════════════════════════════════════
#  HEADLESS AUTHENTICATION
# ═══════════════════════════════════════════════════════════════
def get_fyers_token():
    print("Authenticating with Fyers...")
    req_session = requests.Session()
    req_session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json", "Content-Type": "application/json"})
    fyers_sess = fyersModel.SessionModel(client_id=APP_ID, secret_key=SECRET_KEY, redirect_uri=REDIRECT_URI, response_type="code", grant_type="authorization_code")
    r1 = req_session.post("https://api-t2.fyers.in/vagator/v2/send_login_otp", json={"fy_id": FY_ID, "app_id": "2"})
    req_key1 = r1.json().get("request_key", "")
    totp = pyotp.TOTP(TOTP_KEY).now()
    r2 = req_session.post("https://api-t2.fyers.in/vagator/v2/verify_otp", json={"request_key": req_key1, "otp": totp})
    req_key2 = r2.json().get("request_key", "")
    r3 = req_session.post("https://api-t2.fyers.in/vagator/v2/verify_pin", json={"request_key": req_key2, "identity_type": "pin", "identifier": PIN})
    access_token_temp = r3.json().get("data", {}).get("access_token", "")
    step4_headers = req_session.headers.copy()
    step4_headers["Authorization"] = f"Bearer {access_token_temp}"
    r4 = req_session.post("https://api-t1.fyers.in/api/v3/token", json={"fyers_id": FY_ID, "app_id": APP_ID.split("-")[0], "redirect_uri": REDIRECT_URI, "appType": "200", "response_type": "code", "create_cookie": True}, headers=step4_headers)
    auth_url = r4.json().get("Url", "") or r4.json().get("url", "")
    from urllib.parse import urlparse, parse_qs
    auth_code = parse_qs(urlparse(auth_url).query).get("auth_code", [None])[0]
    fyers_sess.set_token(auth_code)
    resp = fyers_sess.generate_token()
    return resp["access_token"]

# ═══════════════════════════════════════════════════════════════
#  CSV LOGGER SETUP
# ═══════════════════════════════════════════════════════════════
os.makedirs("logs", exist_ok=True)
spot_filename = f"logs/nifty_ticks_{date.today()}.csv"
options_filename = f"logs/options_chain_{date.today()}.csv"

spot_file = open(spot_filename, "a", newline="")
spot_writer = csv.writer(spot_file)
if os.stat(spot_filename).st_size == 0:
    spot_writer.writerow(["Timestamp", "LTP"])

options_file = open(options_filename, "a", newline="")
options_writer = csv.writer(options_file)
if os.stat(options_filename).st_size == 0:
    options_writer.writerow(["Timestamp", "Symbol", "LTP"])

# ═══════════════════════════════════════════════════════════════
#  SMART WEBSOCKET ROUTER
# ═══════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════
#  SMART WEBSOCKET ROUTER (V3 COMPLIANT)
# ═══════════════════════════════════════════════════════════════
def on_message(msg):
    try:
        # Check if the message is a valid tick update ('sf')
        if isinstance(msg, dict) and msg.get("type") == "sf":
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            # Extract directly from the dictionary (No 'data' wrapper)
            sym = msg.get("symbol", "")
            ltp = float(msg.get("ltp", 0) or 0)
            
            if ltp > 0:
                    # STRICT OPTIONS ROUTER: Only save data ending in CE or PE
                    if sym.endswith("CE") or sym.endswith("PE"):
                        options_writer.writerow([now_str, sym, ltp])
                    
            
            # Flush buffers to disk immediately
            spot_file.flush()
            options_file.flush()
    except Exception as e:
        pass

def on_error(msg):
    print("WebSocket Error:", msg)

def on_close(msg):
    print("WebSocket Closed:", msg)
    spot_file.close()
    options_file.close()

def on_connect():
    print(f"Connected! Data streaming to {spot_filename} and {options_filename}...")
    fyers_ws.subscribe(symbols=SYMBOLS, data_type="SymbolUpdate")

if __name__ == "__main__":
    try:
        token = get_fyers_token()
        fyers_ws = FyersDataSocket(
            access_token=f"{APP_ID}:{token}",
            write_to_file=False,
            log_path="logs",
            litemode=False,
            reconnect=True,
            on_message=on_message,
            on_error=on_error,
            on_connect=on_connect,
            on_close=on_close
        )
        fyers_ws.connect()
    except KeyboardInterrupt:
        print("\nShutdown sequence initiated. Closing files safely.")
        spot_file.close()
        options_file.close()
