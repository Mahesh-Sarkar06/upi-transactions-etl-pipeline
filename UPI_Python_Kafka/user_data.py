import random
import uuid
from datetime import datetime, timedelta, timezone

#   --------------------------------------------- BANK CONSTANTS ---------------------------------------------
BANKS = ['State Bank of India', 'HDFC Bank', 'ICICI Bank', 'Axis Bank', 'Kotak Mahindra Bank', 'Bank of Baroda',
         'Punjab National Bank', 'Canara Bank', 'Yes Bank', 'Union Bank of India', 'IDFC First Bank']

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
PAYMENT_GATEWAY = ['Google Pay', 'PayTM', 'Bhim UPI', 'PhonePe', 'Amazon Pay', 'Cred', 'Wallet']


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

# --------------------------------------------- FRIST & LAST NAMES ---------------------------------------------
""" FIRST_NAME = [
    'Abhishek', 'Aman', 'Ajay', 'Arjun', 'Abhimanyu', 'Aditya', 'Ananya', 'Aastha', 'Aishwarya', 'Aditi', 'Arushi', 'Ayushi',
    'Bhanu', 'Bhavesh', 'Bhavya', 'Bhanumati', 'Chandra Shekhar', 'Charu', 'Chitralekha', 'Chirag', 'Chandni', 'Chinmay',
    'Dennis', 'David', 'Divya', 'Dirgha', 'Divyam', 'Diksha', 'Dushyant', 'Disha', 'Elvis', 'Ekta', 'Eklavya', 'Esha',
    'Falguni', 'Fatima', 'Farooq', 'Faiz', 'Gitika', 'Ganesh', 'Gaurav', 'Gayatri', 'Heena', 'Harsh', 'Harshita', 'Harhsit',
    'Ishwar Chandra', 'Ishika', 'Joseph', 'Julie', 'Jitendra', 'Juhi', 'Kanika', 'Kashish', 'Kavita', 'Kamal', 'Keshav', 'Ketan',
    'Lakshmi', 'Lokesh', 'Leena', 'Lucky', 'Mahesh', 'Mahima', 'Manish', 'Mehak', 'Manisha', 'Mayank', 'Mansi', 'Mathew',
    'Nancy', 'Naveen', 'Nupur', 'Nalin', 'Namita', 'Naman', 'Om', 'Prakash', 'Prateek', 'Praveen', 'Pinky', 'Parvati', 'Payal',
    'Rashi', 'Ruhi', 'Rochak', 'Rohan', 'Savitri', 'Shubham', 'Sulochana', 'Sagar', 'Swati', 'Shivam', 'Sangeeta', 'Shyam',
    'Tushar', 'Tanvi', 'Trilochan', 'Tanishq', 'Tushant', 'Tulsi', 'Utsav', 'Urmi', 'Umesh', 'Urvashi', 'Vineet', 'Venkatesh',
    'Vartika', 'Vernika', 'Vaibhav', 'Vaishnavi', 'Varun', 'Zoya'
]

LAST_NAME = [
    'Chaturvedi', 'Trivedi', 'Dwivedi', 'Tiwari', 'Mishra', 'Tyagi', 'Sharma', 'Sarkar', 'Thomas', 'Abraham', 'Rajput',
    'Kumar', 'Shisodia', 'Chauhan', 'Nirwan', 'Bansal', 'Gupta', 'Jain', 'Rawat', 'Negi', 'Pandey', 'Malik', 'Singh',
    'Tripathi', 'Akhtar', 'Khanduri', 'Bisht', 'Pant', 'Iyer', 'Agarwal', 'Saini', 'Roy', 'Chakraborty', 'Sen', 'Kaur',
    'Kapoor', 'Ahuja', 'Dey', 'Kesarwani', 'Srivastava', 'Bhati', 'Bhatt', 'Chawla', 'Chaudhary', 'Yadav', 'Kasana', 'Jha',
    'Bhardwaj', 'Bhattacharya', 'Behl', 'Pathak', 'Goswami', 'Rautela', 'Shanmugham', 'Bose', 'Mittal', 'Shetty'
]"""


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