import os
import win32com.client as win32
from datetime import datetime, timedelta

def get_outlook_signature():
    # Retrieve the default Outlook signature from the user's signature folder
    signature_path = os.path.join(
        os.environ["APPDATA"], 
        "Microsoft", 
        "Signatures"
    )
    if os.path.exists(signature_path):
        for file in os.listdir(signature_path):
            if file.endswith(".htm"):  # Look for the HTML signature file
                with open(os.path.join(signature_path, file), "r", encoding="latin-1") as f:  # Use latin-1 encoding
                    return f.read()
    return ""  # Return an empty string if no signature is found

def send_outlook_email(subject, body, to, cc, attachments):
    # Create an Outlook instance
    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)  # Create a new email item

    # Retrieve the default signature
    default_signature = get_outlook_signature()

    # Set up the email
    mail.Subject = subject
    mail.HTMLBody = body + "<br><br>" + default_signature  # Prepend the custom body to the default signature
    mail.To = to
    mail.CC = cc  # Add CC recipient

    # Attach all files
    for file_path in attachments:
        if os.path.isfile(file_path):
            mail.Attachments.Add(file_path)

    # Send the email
    mail.Send()
    print("Email sent successfully!")

# Vendor-specific directories and recipients
vendor_directories = {
    "Cint": {
        "path": r"T:\MarketInsights\HCMG2008\Kinesis\Incidence Reports\Cint",
        "recipients": ["kat.ramsey@cint.com", "prince.thomas@cint.com", "peter.mancini@cint.com"]
    },
    "Prodege": {
        "path": r"T:\MarketInsights\HCMG2008\Kinesis\Incidence Reports\Prodege and MyPoints",
        "recipients": ["lisa.oconnor@prodege.com", "anupam.kumar@prodege.com", "ketan.lodhiya@prodege.com", "kevin.s@prodege.com", "harshit.singh@prodege.com", "mahesh.b@prodege.com"]
    },
    "Dynata": {
        "path": r"T:\MarketInsights\HCMG2008\Kinesis\Incidence Reports\SSI",
        "recipients": ["Joyce.Lato@Dynata.com"]
    },
    "Toluna": {
        "path": r"T:\MarketInsights\HCMG2008\Kinesis\Incidence Reports\Toluna",
        "recipients": ["Tanu.Bala@Toluna.com", "Rohan.Kapoor@Toluna.com", "Spoc.na@Toluna.com"]
    }
}

# Online Oversample directories (excluding Toluna)
online_oversample_directories = {
    "Cint": {
        "path": r"T:\MarketInsights\HCMG2008\Kinesis\Incidence Reports\Online Oversample\Cint",
    },
    "Prodege": {
        "path": r"T:\MarketInsights\HCMG2008\Kinesis\Incidence Reports\Online Oversample\Prodege and MyPoints",
    },
    "Dynata": {
        "path": r"T:\MarketInsights\HCMG2008\Kinesis\Incidence Reports\Online Oversample\SSI",
    }
}

# Calculate the previous month based on today's date
today = datetime.today()
first_day_of_current_month = today.replace(day=1)
last_month = first_day_of_current_month - timedelta(days=1)
last_month_str = last_month.strftime("%m.%Y")  # Format as MM.YYYY
last_month_name = last_month.strftime("%B %Y")  # Format as "Month Year"

# Function to send emails to vendors
def send_emails_to_vendors(vendor_dirs, online_dirs):
    cc_recipient = "hlannin@nrchealth.com; jzuhur@nrchealth.com"  # Common CC recipients
    for vendor, details in vendor_dirs.items():
        base_path = details["path"]
        recipients = "; ".join(details["recipients"])  # Combine recipients into a single string

        # Construct the folder paths for regular and online oversample files
        regular_folder_path = os.path.join(base_path, "2025", last_month_str)
        online_folder_path = online_dirs.get(vendor, {}).get("path", "")
        online_folder_path = os.path.join(online_folder_path, "2025", last_month_str) if online_folder_path else ""

        # Collect attachments from both folders
        attachments = []
        if os.path.exists(regular_folder_path):
            attachments += [
                os.path.join(regular_folder_path, file)
                for file in os.listdir(regular_folder_path)
                if os.path.isfile(os.path.join(regular_folder_path, file))
            ]
        if os.path.exists(online_folder_path):
            attachments += [
                os.path.join(online_folder_path, file)
                for file in os.listdir(online_folder_path)
                if os.path.isfile(os.path.join(online_folder_path, file))
            ]

        # Send email if there are attachments
        if attachments:
            # Prepare email details
            subject = f"{last_month_name} Incidence Report"
            body = f"""Hello {vendor} Team - Attached please find your {last_month_name} Incidence Report containing all vendor respondents from our {last_month_name} wave.

Thanks,"""

            # Send the email to all recipients in a single email
            send_outlook_email(subject, body, recipients, cc_recipient, attachments)
            print(f"Email sent for {vendor} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")

# Send emails for all vendors, combining regular and online oversample files
send_emails_to_vendors(vendor_directories, online_oversample_directories)
