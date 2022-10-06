import csv
from io import TextIOWrapper
from zipfile import ZipFile
import pandas as pd

file = []

print('READING FILE')
print('...')
with ZipFile('cnpj-qsa copy.zip') as zf:
    with zf.open('ReceitaFederal_QuadroSocietario.csv', 'r') as infile:
        reader = csv.reader(TextIOWrapper(infile, 'utf-8'))
        for row in reader:
            # process the CSV here
            file.append(row)



print('CREATING ARRAY')
print('....')

count=0
rows = []
test=[]
for x in file:
    test.append(x)


print('CREATING LIST OF ROW VALUES')
print('....')

for x in test:
    strng = str(x)[2:-2]
    rows.append(strng.split('\\t'))

print('CREATING DF')
print('....')
column = rows.pop(0)
df = pd.DataFrame(rows,columns = column)

print('WRITING PARQUET')
print(...)

df.to_parquet('altana.parquet')