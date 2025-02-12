from flask_bcrypt import Bcrypt

# Initialize Bcrypt
bcrypt = Bcrypt()


def encrypt_password(password: str) -> str:
    """Encrypts a given string (password) using bcrypt."""
    password_hash = bcrypt.generate_password_hash(password).decode('utf-8')
    return password_hash


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plaintext password against a bcrypt hash."""
    return bcrypt.check_password_hash(hashed_password, plain_password)


if __name__ == "__main__":
    choice = input("Do you want to (1) encrypt a password or (2) verify a hashed password? Enter 1 or 2: ")

    if choice == "1":
        user_input = input("Enter a string to encrypt: ")
        encrypted_output = encrypt_password(user_input)
        print("Encrypted Output:", encrypted_output)

    elif choice == "2":
        plain_text = input("Enter the plaintext password: ")
        hashed_text = input("Enter the bcrypt hashed password: ")

        if verify_password(plain_text, hashed_text):
            print("Password matches the hash!")
        else:
            print("Password does NOT match the hash!")

    else:
        print("Invalid choice. Please enter 1 or 2.")
