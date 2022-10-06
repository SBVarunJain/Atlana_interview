import pandas as pd


def return_df(df, column_name, value):
    df2 = df.where(df.column_name ==value)
    df3=df2.dropna(subset=[column_name])

    return df3




print('SELECT OPTIONS FROM BELOW (PUT NAME IN QUOTES):')
print('0: All operators associated with a given company')
print('1: All companies associated with a given operator')
print('2: All companies connected to a given company via shared operators')
print('any other input will exit the program')
print(' ')
print(' ')
print('---------------------')
print('loading parquet...')

df = pd.read_parquet('altana.parquet', engine='pyarrow')
df=df.reset_index(drop=True)



done = False
while done == False:
    choice = input('Enter your query choice: ')


    if choice ==0:
        try:
            name = input('Enter company name: ')
            df2 = df.where(df.nm_fantasia ==name)
            df3=df2.dropna(subset=['nm_fantasia'])
            
            print(df3['nm_socio'])
        except:
            print('invalid input')


    elif choice == 1:
        try:
            name = input('Enter operator name: ')
            df2 = df.where(df.nm_socio ==name)
            df3=df2.dropna(subset=['nm_socio'])
            print(df3['nm_fantasia'])
        except:
            print('invalid input')

    elif choice == 2:
        try:
            name = input('Enter company name: ')
            df2 = df.where(df.nm_fantasia ==name)
            df3=df2.dropna(subset=['nm_fantasia'])
            operators_df = df3['nm_socio']
            inner_join_df = pd.merge(df, operators_df,on='nm_socio',how='inner')
            companys_df = inner_join_df['nm_fantasia']
            print(companys_df)
        except:
            print('invalid input')

    else:
        done =True


#example inputs
#'MICROLAB SOCIEDADE SIMPLES LTDA - EPP'
#'THIAGO LUCENA DE PAULA AFONSO'
