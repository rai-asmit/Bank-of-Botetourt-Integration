# HubSpot ↔ Fiserv Field Mapping

## Contact Object Mapping

| HubSpot Field Name | Fiserv Field Internal Name | Object | Field Type | Dropdown Select Fields | Notes |
|---|---|---|---|---|---|
| taxidhashed | Tax ID Number | Contact | Single-line text |  | Unique identifier for associations & syncing. Need to hash the value at the integration layer. |
| email | Primary Email | Contact | Single-line text |  | If not present then do not create a contact and/or deal and skip the row. |
| firstname | First Name/Name | Contact | Single-line text |  | Andrew to do through workflow: First name in some instances will be a company name, so when a row has an owner code of 2 - 11, save first name to a new company name property. |
| lastname | Last Name | Contact | Single-line text |  |  |
| date_of_birth | Date of Birth/Age | Contact | Date picker |  | Stop saving data as soon as a ( is reached. So for example we will get back 12/31/1997 (29). In HubSpot we will save 12/31/1997 and the (29) does not map anywhere. |
| date_opened | Date Opened | Contact | Date picker |  |  |
| owner_code | Owner Code | Contact | Single-line text |  | "Andrew to create a workflow for when owner code = 0 - 11 they update a new property ""contact type""<br>0 – Not Assigned<br>1 – Individual/Personal<br>2 – Sole Prop<br>3 – LLC<br>4 – Partnership<br>5 – Corporation<br>6 – Non-profit Incorporated<br>7 – Non-profit Unincorporated<br>8 – Government<br>9 – Estate<br>10 – Bank<br>11 – Trust" |
| contacty_type | N/A | Contact | Single-line text |  | Not in Fiserv Integration |
| br | Br | Contact |  |  | "Andrew to create a workflow that when BR is one of the values below, we update a new property ""Branch Location"" to the below:<br>1 – Buchanan<br>2 – Daleville Town Center<br>3 – Eagle Rock<br>4 – Troutville<br>11 – Lexington<br>12 – Fairfield<br>21 – Bonsack<br>22 – Peters Creek<br>23 – Vinton<br>24 – Cave Spring<br>31 – Smith Mountain Lake<br>32 – Rocky Mount<br>41 – Salem<br>51 – Melrose" |
| branch_location | N/A | Contact | Single-line text |  | Not in Fiserv Integration |
| adress | Address Line 2 | Contact | Single-line text |  |  |
| address2 | Address Line 3 | Contact | Single-line text |  |  |
| city | City | Contact | Single-line text |  |  |
| state | State | Contact | Single-line text |  |  |
| zip | Zip Code | Contact | Single-line text |  |  |
| number_of_dda_accounts | DDA Accts | Contact | Number |  |  |
| number_of_cd_accounts | CD Number of Accounts | Contact | Number |  |  |
| total_deposits | Total Deposits | Contact | Number |  |  |
| number_of_loan_accounts | Loan Number of Accounts | Contact | Number |  |  |
| total_number_of_loans | Total Loans | Contact | Number |  |  |
| int_bank_one | Int. Bank One | Contact | Single checkbox |  | Andrew to create a workflow for when this is 1 we update a new property Enrolled in Online Banking to Yes. When it's 99, it's no. |
| enrolled_in_online_banking | N/A | Contact | Dropdown select | Yes, No | Not in Fiserv Integration |
| User | User Defined Three | Contact | Single-line text |  | Andrew to create workflow for when this is yes, it marks enrolled in mobile banking to yes. When it's blank or no it marks it as no. |
| enrolled_in_mobile_banking | N/A | Contact | Dropdown select | Yes, No |  |
| empl | Empl Code | Contact | Single checkbox |  | "Andrew to create a workflow that updates a new property employee/institution relationship, for when the following values are received it updates accordingly:<br>00 = not employee<br>01 = Officer<br>02 = Director<br>03 = Bank Owned account<br>04 = Employee<br>05 – Other Financial Institution" |
| employeeinstitution_relationship | N/A | Contact | Single checkbox |  |  |

---

## Deal Object Mapping — DDA

| HubSpot Field Name | Fiserv Field Internal Name | Object | Field Type | Dropdown Select Fields | Notes |
|---|---|---|---|---|---|
| taxidhashed | Tax ID Number | Deal | Single-line text | N/A | Unique identifier for associations & syncing. Need to hash the value at the integration layer. |
| account_type | Type Code External Description | Deal | Single-line text | N/A | Account Type > Andrew D. to add automation to simplify data digestion and deal naming within HubSpot |
| date_opened | Date Opened | Deal | Date picker | N/A |  |
| account_status | Status Desc | Deal | Single-line text | Active, Credits, New, Pending, Locked, Dormant, Closed |  |
| current_balance | Current Balance | Deal | Number | N/A |  |
| delivery_code | Delivery Code | Deal | Dropdown select | N/A |  |
| last_deposit_amount | Amount Last Deposit | Deal | Number | N/A |  |
| last_withdrawal_amount | Amount Last Withdrawl | Deal | Number | N/A |  |
| date_closed | Date Closed | Deal | Date picker | N/A |  |
| account_last_4 | Account Number Masked | Deal | Single-line text | N/A |  |

---

## Deal Object Mapping — CD

| HubSpot Field Name | Fiserv Field Internal Name | Object | Field Type | Dropdown Select Fields | Notes |
|---|---|---|---|---|---|
| taxidhashed | Tax ID Number | Deal | Single-line text | N/A | Unique identifier for associations & syncing. Need to hash the value at the integration layer. |
| type_code_external_description | Type Code External Description | Deal | Single-line text | N/A |  |
| date_opened | Date Opened | Deal | Date picker | N/A |  |
| account_status | Status Desc | Deal | Single-line text | Active, Credits, New, Pending, Locked, Dormant, Closed |  |
| current_balance | Current Balance | Deal | Number | N/A |  |
| delivery_code | Delivery Code | Deal | Single-line text | N/A |  |
| openmat_balance | Open/Mat Balance | Deal | Number | N/A |  |
| date_closed | Date Closed | Deal | Date picker | N/A |  |
| account_last_4 | Account Number Masked | Deal | Single-line text | N/A |  |

---

## Deal Object Mapping — LNA

| HubSpot Field Name | Fiserv Field Internal Name | Object | Field Type | Dropdown Select Fields | Notes |
|---|---|---|---|---|---|
| taxidhashed | Tax ID Number | Deal | Single-line text | N/A | Unique identifier for associations & syncing. Need to hash the value at the integration layer. |
| type_code_external_description | Type Code External Description | Deal | Single-line text | N/A |  |
| date_opened | Date Opened | Deal | Date picker | N/A |  |
| account_status | Status Desc | Deal | Single-line text | Active, Credits, New, Pending, Locked, Dormant, Closed |  |
| current_balance | Current Balance | Deal | Number | N/A |  |
| opening_advance | Opening Advance | Deal | Number | N/A |  |
| date_closed | Date Closed | Deal | Date picker | N/A |  |
| account_last_4 | Account Number Masked | Deal | Single-line text | N/A |  |

---

## Deal Object Mapping — SDA

| HubSpot Field Name | Fiserv Field Internal Name | Object | Field Type | Dropdown Select Fields | Notes |
|---|---|---|---|---|---|
| taxidhashed | Tax ID Number | Deal | Single-line text | N/A | Unique identifier for associations & syncing. Need to hash the value at the integration layer. |
| type_code_external_description | Type Code External Description | Deal | Single-line text | N/A |  |
| date_opened | Date Opened | Deal | Date picker | N/A |  |
| account_status | Status Desc | Deal | Single-line text | Active, Credits, New, Pending, Locked, Dormant, Closed |  |
| date_closed | Date Closed | Deal | Date picker | N/A |  |
| account_last_4 | Account Number Masked | Deal | Single-line text | N/A |  |
| current_balance | Current Balance | Deal | Number | N/A |  |
| delivery_code | Delivery Code | Deal | Single-line text | N/A |  |
| openmat_balance | Open/Mat Balance | Deal | Number | N/A |  |

---

# Integration Overview

Source System: Fiserv Precision  
Transport: Fiserv MoveIt drops a nightly file onto a bank-provided server  
Loader: digitalJ2-built process on that server reads the file, applies sync rules, and calls the HubSpot API  
Target System: HubSpot — Contacts and associated Deals, keyed by a custom property Tax ID Hash (hashed; true value never stored for security)

## Key Identifier

Tax ID Number is the primary unique identifier across all tables.  
For security purposes, the Tax ID number will never be stored directly in HubSpot. Instead, a hashed version of the Tax ID will be stored and used for all matching and association logic.

## Composite Key for Account Matching (DDA, CD, LNA, SDA)

When matching or creating account records (Deals) in HubSpot, three fields are used together as a composite key to uniquely identify each account:

1. Tax ID Hash — identifies which contact the account belongs to
2. Date Opened — identifies when the account was opened
3. Account Last 4 (Account Number Masked) — distinguishes accounts opened on the same date by the same customer, or accounts opened on the same date by different customers

⚠ Why all three? Using Tax ID + Date Opened alone risks a collision if two customers open accounts on the same day, or if one customer opens multiple accounts on the same day. The Last 4 of the account number makes the composite key truly unique.

## File Processing Order

Files are processed in the following order each night:

1. CIF → Contact records (source of truth for all contact data)
2. DDA → Associated Deal
3. CD → Associated Deal
4. LNA → Associated Deal
5. SDA → Associated Deal

## CIF Table Processing (Contact Sync)

Each row in the CIF file represents a potential contact record. A single customer may appear on multiple rows if they hold multiple accounts — this is expected behavior and does not indicate duplicate contacts.

### Step 1 — Hash the Tax ID

For each row in the CIF file:

- Generate a hash of the Tax ID Number
- Use the hashed value to search for a matching contact in HubSpot

### Route A — Existing Contact Found (Hash Match)

If a contact with a matching Tax ID hash already exists in HubSpot:

1. Update the existing contact record with any new or changed data from the CIF row.
2. Proceed to account table processing for DDA, CD, LNA, and SDA (see sections below).

### Route B — Tax ID Found but Email is Unknown

If the CIF row contains a Tax ID but Primary Email is missing or unknown:

- Skip the row entirely
- Do not create or update any contact or account record in HubSpot
- This prevents creating incomplete or unusable contact records.

### Route C — New Contact (No Hash Match)

If the Tax ID hash does NOT exist in HubSpot AND a valid email address is present:

1. Create a new contact record in HubSpot
2. Store the hashed Tax ID on the contact
3. Populate all contact fields from the CIF row
4. Proceed to account table processing for DDA, CD, LNA, and SDA (see sections below).

## Account Table Processing (DDA, CD, LNA, SDA — Deal Sync)

The following logic applies identically to all four account tables: DDA, CD, LNA, and SDA. Each account is stored as a Deal in HubSpot and associated to its parent contact.

### Step 1 — Look Up the Contact

Using the Tax ID Hash from the account row, locate the associated contact in HubSpot.

- If no matching contact is found, skip the row. (The contact should have been created during CIF processing.)

### Step 2 — Match Using Composite Key

Search the contact's associated Deals in HubSpot for a record that matches all three of the following:

- Tax ID Hash (confirms correct contact)
- Date Opened (identifies the account open date)
- Account Last 4 (Account Number Masked — distinguishes accounts with the same open date)

### Route A — No Matching Deal Found → CREATE

If no existing Deal matches the composite key:

1. Create a new Deal record in HubSpot
2. Populate all Deal fields from the account row
3. Associate the Deal to the contact

### Route B — Matching Deal Found → UPDATE

If an existing Deal matches all three composite key fields (Tax ID Hash + Date Opened + Account Last 4):

1. Update all Deal properties with the latest values from the account row
2. Move to the next row

## Initial Full Import vs. Nightly Sync

### Initial Full Import (Go-Live)

The project will begin with a full-file initial import. At go-live, the bank will provide a complete export of all active customer relationships. The loader will process this file in an initial load mode, applying the same identity and matching rules described above.

During this run:

- Existing HubSpot contacts will be updated
- New contacts will be created as needed
- All fields will be populated — identity fields, product summary fields, branch, customer type, digital adoption indicators, and marketing preferences

### Nightly Incremental Sync

From go-live forward, the nightly integration functions as an incremental update. Each new data file is processed according to the same rules above, with updates applied to HubSpot only where appropriate.

- The nightly load keeps HubSpot contact and deal properties aligned with the core banking system (Fiserv)
- Data will drop into Fiserv at 12:07 AM EST