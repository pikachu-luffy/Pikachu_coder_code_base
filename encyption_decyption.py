import pandas as pd
from cryptography.fernet import Fernet

# Generate a key (only do this once, store securely!)
key = Fernet.generate_key()
cipher = Fernet(key)

print("Encryption Key (save this securely!):", key.decode())

# Example CSV data (in practice, you'd load from file)
data = {
    "name": ["Alice", "Bob"],
    "phone": ["1234567890", "9876543210"],
    "email": ["alice@example.com", "bob@example.com"]
}
df = pd.DataFrame(data)

# Encrypt sensitive columns
df["phone_encrypted"] = df["phone"].apply(lambda x: cipher.encrypt(x.encode()).decode())
df["email_encrypted"] = df["email"].apply(lambda x: cipher.encrypt(x.encode()).decode())

# Drop original sensitive columns if needed
df = df.drop(columns=["phone", "email"])
print("\nEncrypted DataFrame:\n", df)

# ----- Later: Decrypt -----
df["phone_decrypted"] = df["phone_encrypted"].apply(lambda x: cipher.decrypt(x.encode()).decode())
df["email_decrypted"] = df["email_encrypted"].apply(lambda x: cipher.decrypt(x.encode()).decode())

print("\nDecrypted DataFrame:\n", df)



# for the CSV file

# df = pd.read_csv("your_file.csv")

# # Encrypt
# df["phone"] = df["phone"].apply(lambda x: cipher.encrypt(x.encode()).decode())
# df["email"] = df["email"].apply(lambda x: cipher.encrypt(x.encode()).decode())

# df.to_csv("encrypted_file.csv", index=False)

# # Decrypt
# df = pd.read_csv("encrypted_file.csv")
# df["phone"] = df["phone"].apply(lambda x: cipher.decrypt(x.encode()).decode())
# df["email"] = df["email"].apply(lambda x: cipher.decrypt(x.encode()).decode())