import pandas as pd
import numpy as np
import imaplib
import email
import re
import warnings
from pathlib import Path
pd.options.mode.chained_assignment = None

# folder path
downloads_path = str(Path.home() / "Downloads")
path1 = downloads_path

# connect to inbox
imap_server = imaplib.IMAP4_SSL(host='imap.gmail.com')
imap_server.login('x', 'y')
imap_server.select()

dff1 = []
dff4 = []
# Retrieve emails
_, message_numbers_raw = imap_server.search(None, 'ALL')
for message_number in message_numbers_raw[0].split():
    _, msg = imap_server.fetch(message_number, '(RFC822)')

    message = email.message_from_bytes(msg[0][1])
    html1 = {message.get_payload(decode=True)}
    m1 = re.findall(r'\s97.{11}\s', str(html1))
    for i in m1:
        dff1.append(i)
    dffw = pd.DataFrame(dff1, index=None)
    m2 = re.findall(r'http[^\\]+', str(html1))
    for i in m2:
        dff4.append(i)
    dffz = pd.DataFrame(dff4, index=None)

dffF = pd.concat([dffw, dffz], axis=1)
dffF.dropna(how='any', inplace=True)
dffF.columns = ['ISBN_y', 'URL']
dffF['URL'] = dffF['URL'].str.lstrip()
dffF['URL'] = dffF['URL'].str.split(' ')
dffF['URL'] = dffF['URL'].str[0]
dffF['ISBN_y'] = pd.to_numeric(dffF['ISBN_y'])

# Read 2 Excel files
with warnings.catch_warnings(record=True):
    warnings.simplefilter("always")
    almaPorts = pd.read_excel(path1 + "/almaPorts.xlsx", engine="openpyxl")
    almaTitles = pd.read_excel(path1 + "/almaTitles.xlsx", engine="openpyxl")
    email = dffF

# Join files
inner_join_df = almaPorts.merge(almaTitles, how="inner", on="MMS ID")
inner_join_df['ISBN_y'] = inner_join_df['ISBN_y'].replace({r'[^0-9]': ''}, regex=True)
inner_join_df['ISBN_y'] = pd.to_numeric(inner_join_df['ISBN_y'])
inner_join_df2 = inner_join_df.merge(email, how="inner", on="ISBN_y")
inner_join_df2['ISBN_y'] = inner_join_df2['ISBN_y'].astype(str)
inner_join_df2['Portfolio ID'] = inner_join_df2['Portfolio ID'].astype(str)
inner_join_df2['MMS ID'] = inner_join_df2['MMS ID'].astype(str)
inner_join_df2['ISBN_y'] = inner_join_df2['ISBN_y'].str[:13]
inner_join_df3 = inner_join_df2[['Name', 'Portfolio ID', 'MMS ID', 'ISBN_y', 'title_x', 'URL']]
df4 = inner_join_df3.sort_values('URL')

# export master list
df4.to_excel(path1 + "/1EbookProcessing.xlsx", index=None)

# export individual collections
dfISBNurls = inner_join_df3[['ISBN_y', 'URL']]
dfISBNurls['URL'] = dfISBNurls['URL'].str.replace('\n', '')
dfCam = dfISBNurls[dfISBNurls.URL.str.contains('cambridge|/doi.org', case=False)]
if dfCam.empty is False:
    np.savetxt(path1 + "/2cambridge.txt", dfCam, fmt="%s", delimiter="\t")
dfDG = dfISBNurls[dfISBNurls.URL.str.contains('degruyter', case=False)]
if dfDG.empty is False:
    np.savetxt(path1 + "/3degruyter.txt", dfDG, fmt="%s", delimiter="\t")
dfEBS = dfISBNurls[dfISBNurls.URL.str.contains('ebscohost', case=False)]
if dfEBS.empty is False:
    np.savetxt(path1 + "/4ebsco.txt", dfEBS, fmt="%s", delimiter="\t")
dfJSTOR = dfISBNurls[dfISBNurls.URL.str.contains('jstor', case=False)]
if dfJSTOR.empty is False:
    np.savetxt(path1 + "/5jstor.txt", dfJSTOR, fmt="%s", delimiter="\t")
dfUPSO = dfISBNurls[dfISBNurls.URL.str.contains('/dx.doi.org', case=False)]
if dfUPSO.empty is False:
    np.savetxt(path1 + "/6upso.txt", dfUPSO, fmt="%s", delimiter="\t")
dfmuse = dfISBNurls[dfISBNurls.URL.str.contains('muse.jhu.edu', case=False)]
if dfmuse.empty is False:
    np.savetxt(path1 + "/7projectmuse.txt", dfmuse, fmt="%s", delimiter="\t")
dfPQ = dfISBNurls[dfISBNurls.URL.str.contains('proquest', case=False)]
if dfPQ.empty is False:
    np.savetxt(path1 + "/8proquest.txt", dfPQ, fmt="%s", delimiter="\t")
dfTF = dfISBNurls[dfISBNurls.URL.str.contains('taylorfrancis', case=False)]
if dfTF.empty is False:
    np.savetxt(path1 + "/9taylorfrancis.txt", dfTF, fmt="%s", delimiter="\t")

# export portfolios
dfPorts = inner_join_df3[['Portfolio ID', 'URL']]
dfCamP = dfPorts[dfPorts.URL.str.contains('cambridge|/doi.org', case=False)]
if dfCamP.empty is False:
    np.savetxt(path1 + "/2cambridgePORTS.txt", dfCamP['Portfolio ID'], fmt="%s", delimiter="\t", header="Portfolio ID",
               comments='')
dfDGP = dfPorts[dfPorts.URL.str.contains('degruyter', case=False)]
if dfDGP.empty is False:
    np.savetxt(path1 + "/3degruyterPORTS.txt", dfDGP['Portfolio ID'], fmt="%s", delimiter="\t", header="Portfolio ID",
               comments='')
dfEBSP = dfPorts[dfPorts.URL.str.contains('ebscohost', case=False)]
if dfEBSP.empty is False:
    np.savetxt(path1 + "/4ebscoPORTS.txt", dfEBSP['Portfolio ID'], fmt="%s", delimiter="\t", header="Portfolio ID",
               comments='')
dfJSTORP = dfPorts[dfPorts.URL.str.contains('jstor', case=False)]
if dfJSTORP.empty is False:
    np.savetxt(path1 + "/5jstorPORTS.txt", dfJSTORP['Portfolio ID'], fmt="%s", delimiter="\t", header="Portfolio ID",
               comments='')
dfUPSOP = dfPorts[dfPorts.URL.str.contains('/dx.doi.org', case=False)]
if dfUPSOP.empty is False:
    np.savetxt(path1 + "/6upsoPORTS.txt", dfUPSOP['Portfolio ID'], fmt="%s", delimiter="\t", header="Portfolio ID",
               comments='')
dfmuseP = dfPorts[dfPorts.URL.str.contains('muse.jhu.edu', case=False)]
if dfmuseP.empty is False:
    np.savetxt(path1 + "/7projectmusePORTS.txt", dfmuseP['Portfolio ID'], fmt="%s", delimiter="\t",
               header="Portfolio ID", comments='')
dfPQP = dfPorts[dfPorts.URL.str.contains('proquest', case=False)]
if dfPQP.empty is False:
    np.savetxt(path1 + "/8proquestPORTS.txt", dfPQP['Portfolio ID'], fmt="%s", delimiter="\t", header="Portfolio ID",
               comments='')
dfTFP = dfPorts[dfPorts.URL.str.contains('taylorfrancis', case=False)]
if dfTFP.empty is False:
    np.savetxt(path1 + "/9taylorfrancisPORTS.txt", dfTFP['Portfolio ID'], fmt="%s", delimiter="\t",
               header="Portfolio ID", comments='')
