import random
import uuid
from datetime import datetime, timedelta, timezone

#   --------------------------------------------- BANK CONSTANTS ---------------------------------------------
BANKS = ['State Bank of India', 'HDFC Bank', 'ICICI Bank', 'Axis Bank', 'Kotak Mahindra Bank', 'Bank of Baroda',
         'Bank of India', 'Punjab National Bank', 'Canara Bank', 'Yes Bank', 'Union Bank of India', 'IDFC First Bank',
         'Indian Bank', 'IndusInd Bank', 'Central Bank of India', 'Bank of Maharashtra', 'UCO Bank']

#   --------------------------------------------- BANK IFSC CODE ---------------------------------------------
BANK_IFSC = {
    'State Bank of India': 'SBIN',
    'HDFC Bank': 'HDFC',
    'ICICI Bank': 'ICIC',
    'Axis Bank': 'UTIB',
    'Kotak Mahindra Bank': 'KKBK',
    'Bank of Baroda': 'BARB',
    'Punjab National Bank': 'PUNB',
    'Canara Bank': 'CNRB',
    'Yes Bank': 'YESB',
    'Union Bank of India': 'UBIN',
    'IDFC First Bank': 'IDFB',
    'Indian Bank': 'IDIB',
    'Bank of India': 'BKID',
    'IndusInd Bank': 'INDB',
    'Central Bank of India': 'CBIN',
    'Bank of Maharashtra': 'MAHB',
    'UCO Bank': 'UCBA'
}

#   --------------------------------------------- PAYMENT GATEWAYS ---------------------------------------------
PAYMENT_GATEWAY = ['Google Pay', 'PayTM', 'Bhim UPI', 'Net Banking', 'PhonePe', 'Amazon Pay', 'Cred', 'Wallet', 'RazorPay']


#   --------------------------------------------- PAYMENT GATEWAYS ---------------------------------------------
CATEGORY = [
    'Grocery', 'Ticket Booking', 'Hotel Booking', 'Restaurant', 'Food Court', 'Online', 'Clothings', 'Footwears',
    'Bill Payments', 'Recharge', 'Money Transfer', 'Medicines'
]


#   --------------------------------------------- INDIAN STATES ---------------------------------------------
STATES = [
    'Arunachal Pradesh', 'Andhra Pradesh', 'Assam', 'Bihar', 'Chhattisgarh', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh',
    'Jharkhand', 'Jammu & Kashmir', 'Karnataka', 'Kerala', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
    'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh', 'Uttarakhand',
    'West Bengal', 'Chandigarh', 'Delhi', 'Ladakh', 'Daman & Diu', 'Dadra & Nagar Haveli'
]


# --------------------------------------------- RANDOM IFSC GENERATOR ---------------------------------------------
def ifsc_generator(bank_name):
    prefix = BANK_IFSC.get(bank_name, 'XXXX')
    branch_code = f'{random.randint(0, 999999):06d}'

    return f'{prefix}0{branch_code}'


# --------------------------------------------- RANDOM AMOUNT GENERATOR ---------------------------------------------
def random_amount():
    base = int(random.expovariate(1/1000))
    
    if base == 0:
        base = random.randint(0, 50)

    return min(max(base, 1), 100000)


# --------------------------------------------- TRANSACTION GENERATOR ---------------------------------------------
def transaction_generator(status='completed', gateway=None, timestamp=None):
    tx_id = str(uuid.uuid4())
    bankName = random.choice(BANKS)

    txn = {
        "transaction_id": tx_id,
        "bank_name": bankName,
        "ifsc_code": ifsc_generator(bankName),
        "amount": random_amount(),
        "category": random.choice(CATEGORY),
        "payment_gateway": (gateway or random.choice(PAYMENT_GATEWAY)),
        "state": random.choice(STATES),
        "status": status,
        "timestamp": (timestamp or datetime.now()).isoformat() + 'Z'
    }

    return txn


# --------------------------------------------- USER AND TRANSACTION ---------------------------------------------
class UPITransactionGenerator:
    def __init__(self, n_users=100, failed_pct=0.02, seed=None):
        if seed is not None:
            random.seed(seed)

        self.n_users = n_users
        self.failed_pct = failed_pct

    
    def generate_transaction(self, count=500, start_time=None, time_span_hours=24):
        startTime = start_time or datetime.now(timezone.utc)

        for _ in range(count):
            status = 'failed' if random.random() < self.failed_pct else 'completed'
            offset_sec = random.randint(0, int(time_span_hours * 3600))
            ts = startTime - timedelta(seconds=offset_sec)
            gateway = random.choice(PAYMENT_GATEWAY)
            txn = transaction_generator(gateway=gateway, status=status, timestamp=ts)
            yield txn