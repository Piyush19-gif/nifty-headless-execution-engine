# nifty-headless-execution-engine

A headless, cloud-deployable Python execution engine for Nifty 50 options scalping. Features automated TOTP auth, real-time data structuring for Tuesday expiries, and dynamic risk management capped at a 5-lakh capital limit.

## Architecture Highlights
* **Headless TOTP Authentication:** Programmatic bypass of manual broker logins via pyotp and Fyers API v3.
* **Deterministic State Persistence:** JSON-based state recovery to survive cloud server drops and auto-resume execution.
* **Microstructure Risk Management:** Real-time VIX polling to dynamically scale lot sizes.
* **Daemon Thread Reconciliation:** 120-second background polling to eliminate duplicate risk exposure and "ghost" orders.
* **Concurrent Data Structuring:** Real-time tick-by-tick WebSocket routing that structures Nifty Spot and 40 Options strikes simultaneously.

---

## Ecosystem Impact & Industry Validation

This architecture was open-sourced to address the severe latency and authentication bottlenecks faced by retail algorithmic traders. Within days of release, the execution framework generated massive traction across the quantitative finance community, achieving over **29,000 organic impressions** and initiating direct architectural reviews from institutional trading desks, retail brokerage leadership, and core open-source maintainers.

**<img width="632" height="488" alt="Screenshot 2026-05-26 152516" src="https://github.com/user-attachments/assets/6900c4b7-47bd-40c9-a02d-bc10746f1b9c" />**

**<img width="498" height="459" alt="image" src="https://github.com/user-attachments/assets/c69c033a-b0be-4b49-aa3a-8c05d124528b" />**
**<img width="490" height="481" alt="image" src="https://github.com/user-attachments/assets/d8c0f10c-e953-40e4-81d5-9b1275037a09" />**


**<img width="495" height="517" alt="image" src="https://github.com/user-attachments/assets/6a829f32-07a7-4dba-9bd7-5068c74a5acd" />**

**<img width="501" height="518" alt="image" src="https://github.com/user-attachments/assets/72e7d22c-58ce-420f-bd9c-3ae4eed2e620" />**
