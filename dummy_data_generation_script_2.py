import hashlib
import csv
import random
from datetime import datetime, timedelta
from dateutil.tz import gettz

def generate_dummy_data():
    """Generate dummy data CSV with phone numbers and their SHA256 hashes according to new schema"""
    
    # Phone numbers provided
    phone_numbers = [
        "9873185132", "9997297420", "8090780377", "7017890375", "7409898048",
        "7906063900", "7906310216", "9927173568", "8046396150", "8851872499",
        "7619977823", "9958446833", "8460779976", "8035059316", "9458744602",
        "8390690180", "8047943528", "9627363411", "7505720414", "9228833947", 
        "9178494050", "8279679317", "9422330972", "7016294998", "5678456012", 
        "4444333351", "5901234567", "5555666666", "1239087651"
    ]
    
    # Email addresses provided
    email_addresses = [
        "jokerrobbat@gmail.com", "narutouzumakichauhan1995@gmail.com", "pikachuashchut@gmail.com", "pikachuashchut5@gmail.com", 
        "karnamohanta7@gmail.com", "shubashkumar2021@gmail.com", "sanketnigudkar@gmail.com", "sisodiyapradipsinh2@gmail.com", 
        "roronoazorochauhan1995@gmail.com", "gonkilluachauhan1995@gmail.com", "monkeydluffychauhan1995@gmail.com"
    ]
    
    # Extra columns for testing error columns phenomena
    nick_names = ["teenage", "old", "newborn", "mildborn"]
    religion_names = ['hindu', 'christian', 'jain', 'buddhist']
    consent_values = ['0', '1']
    
    # Sample data pools for generating realistic dummy data
    first_names = ["Rahul", "Priya", "Amit", "Sneha", "Vikram", "Anita", "Rajesh", "Kavya", "Suresh", "Meera", 
                   "Arjun", "Pooja", "Kiran", "Deepika", "Manoj", "Ritu", "Sanjay", "Nisha"]
    
    last_names = ["Sharma", "Patel", "Singh", "Kumar", "Gupta", "Agarwal", "Jain", "Verma", "Shah", "Mehta",
                  "Reddy", "Nair", "Iyer", "Chopra", "Malhotra", "Bansal", "Saxena", "Tiwari"]
    
    cities = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Kolkata", "Hyderabad", "Pune", "Ahmedabad", 
              "Jaipur", "Lucknow", "Kanpur", "Nagpur", "Indore", "Bhopal", "Visakhapatnam", "Patna"]
    
    states = ["Maharashtra", "Delhi", "Karnataka", "Tamil Nadu", "West Bengal", "Telangana", "Gujarat", 
              "Rajasthan", "Uttar Pradesh", "Madhya Pradesh", "Andhra Pradesh", "Bihar", "Kerala", "Punjab"]
    
    genders = ["Male", "Female", "Other"]
    sources = ["Website", "Mobile App", "Social Media", "Email Campaign", "Referral", "Advertisement"]
    
    brands = ["Cadbury", "Oreo", "Bournvita", "5Star", "Gems", "Dairy Milk", "Silk"]
    frequencies = ["Daily", "Weekly", "Monthly", "Occasionally", "Rarely"]
    bool_values = [True, False]  # For BOOL type columns
    preferences = ["Sweet", "Bitter", "Milk Chocolate", "Dark Chocolate", "White Chocolate"]
    liked_recipes = ["Chocolate Cake", "Brownies", "Cookies", "Ice Cream", "Pudding"]
    isscan_values = ["Yes", "No"]
    packtype_values = ["Pouch", "Box", "Wrapper", "Bottle", "Can"]
    created_story_values = ["Adventure", "Romance", "Mystery", "Comedy", "Drama"]
    cricket_answers = ["Batting", "Bowling", "Fielding", "All-rounder", "Captain"]
    coupon_flag_values = ["Yes", "No"]
    
    # CSV header based on new schema (added email column)
    header = [
        "first_name", "last_name",  "phone", "email",
        "google_advertiser_id__gaid_", "apple_id_for_advertisers__idfa_",
        "consent_timestamp", "date_of_birth", "city", "gender", "state", "source", 
        "what_is_your_favourite_brand_", "how_often_do_you_consume_biscuits_", 
        "are_you_a_parent", "liked_recipes", "isscan", "zip", "packtype", 
        "chocolate_consumption", "created_story", "jio_gems_cricket_answer", 
        "cadbury_brand", "mithai_consumption_flag", "candies_consumption_flag", 
        "biscuit_consumption_flag", "birth_year", "coupon_flag",
          "nick_name", "religion"
    ]
    
    # Generate data rows with different combinations
    rows = []
    
    # Create a larger pool by combining phone numbers and emails
    # We'll create different combinations: phone only, email only, both
    max_records = max(len(phone_numbers), len(email_addresses)) * 2  # Generate more records
    
    for i in range(max_records):
        # Determine what type of record to create
        record_type = random.choice(['phone_only', 'email_only', 'both'])
        
        # Initialize phone and email variables
        phone = ""
        email = ""
        phone_hashed = ""
        email_hashed = ""
        
        if record_type == 'phone_only':
            # Only phone, no email
            phone = random.choice(phone_numbers)
            # phone_hashed = hashlib.sha256(phone.encode('utf-8')).hexdigest()
            email = ""  # Empty email
            # email_hashed = ""  # Empty email hash
            
        elif record_type == 'email_only':
            # Only email, no phone
            email = random.choice(email_addresses)
            # email_hashed = hashlib.sha256(email.encode('utf-8')).hexdigest()
            phone = ""  # Empty phone
            # phone_hashed = ""  # Empty phone hash
            
        else:  # both
            # Both phone and email
            phone = random.choice(phone_numbers)
            email = random.choice(email_addresses)
            # phone_hashed = hashlib.sha256(phone.encode('utf-8')).hexdigest()
            # email_hashed = hashlib.sha256(email.encode('utf-8')).hexdigest()
        
        # Extra columns for testing
        nick_name = random.choice(nick_names)
        religion = random.choice(religion_names)
        consent_data = random.choice(consent_values)
        
        # Generate proper timestamp format for TIMESTAMP type
        consent_timestamp = datetime.now(tz=gettz('UTC')).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Generate other dummy data
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        # Generate birth year (age 18-65) as INT64
        birth_year = random.randint(1959, 2006)  # Current year 2024 - 65 to 2024 - 18
        
        # Generate proper date format for DATE type (YYYY-MM-DD)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # Safe day range for all months
        date_of_birth = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
        
        # Generate random zip code (6 digits)
        zip = f"{random.randint(100000, 999999)}"
        
        # Generate random GAID and IDFA (UUID-like format)
        gaid = f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}"
        idfa = f"{random.randint(10000000, 99999999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(100000000000, 999999999999)}"
        
        row = [
            first_name,
            last_name,
            phone,  # Phone column (can be empty)
            email,  # Email column (can be empty)
            # phone_hashed,  # Phone hash (can be empty)
            # email_hashed,  # Email hash (can be empty)
            gaid,  # google_advertiser_id__gaid_
            idfa,  # apple_id_for_advertisers__idfa_
            # consent_data,
            consent_timestamp,
            date_of_birth,  # DATE format
            random.choice(cities),
            random.choice(genders),
            random.choice(states),
            random.choice(sources),
            random.choice(brands),  # what_is_your_favourite_brand_
            random.choice(frequencies),  # how_often_do_you_consume_biscuits_
            random.choice(bool_values),  # are_you_a_parent (BOOL)
            random.choice(liked_recipes),
            random.choice(isscan_values),
            zip,
            random.choice(packtype_values),
            random.choice(frequencies),  # chocolate_consumption
            random.choice(created_story_values),
            random.choice(cricket_answers),  # jio_gems_cricket_answer
            random.choice(preferences),  # cadbury_brand
            random.choice(bool_values),  # mithai_consumption_flag (BOOL)
            random.choice(bool_values),  # candies_consumption_flag (BOOL)
            random.choice(bool_values),  # biscuit_consumption_flag (BOOL)
            birth_year,  # INT64
            random.choice(coupon_flag_values),
            nick_name,  # Extra column for testing
            religion   # Extra column for testing
        ]
        
        rows.append(row)
    
    # Write to CSV file
    output_file = "dummy_extra_data_records_try_4.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(rows)
    
    # Count different record types for reporting
    phone_only_count = sum(1 for row in rows if row[3] and not row[2])  # phone but no email
    email_only_count = sum(1 for row in rows if row[2] and not row[3])  # email but no phone
    both_count = sum(1 for row in rows if row[2] and row[3])  # both email and phone
    
    print(f"Generated {len(rows)} rows of dummy data in '{output_file}'")
    print(f"  - Phone only: {phone_only_count} records")
    print(f"  - Email only: {email_only_count} records")
    print(f"  - Both phone and email: {both_count} records")
    print(f"Sample phone number hash: {phone_numbers[0]} -> {hashlib.sha256(phone_numbers[0].encode('utf-8')).hexdigest()}")
    print(f"Sample email hash: {email_addresses[0]} -> {hashlib.sha256(email_addresses[0].encode('utf-8')).hexdigest()}")
    print("Schema updates applied:")
    print("- Added email column with predefined email addresses")
    print("- Created mixed combinations of phone/email data")
    print("- Maintained consistent column count with empty strings for missing data")
    print("- Updated column names to match new schema")
    print("- BOOL columns now use True/False values")
    print("- DATE column uses YYYY-MM-DD format")
    print("- TIMESTAMP column uses proper UTC format")
    print("- Added birth_year as INT64")
    print("- Kept nick_name and religion columns for error testing")
    
    return output_file

if __name__ == "__main__":
    generate_dummy_data()