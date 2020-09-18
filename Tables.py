def options_table(data):
  for price in data:
    price_value = price['Value']
    day = price['Time'].day
    yield {'Value': price_value, 'Time': price['Time'], 'SN': price['SN'], 'increment': 0}
    for increment in price['increments']:
      if increment['Value'] == 0: continue
      price_value += increment['Value']
      yield {
        'Value': price_value,
        'Time': increment['Time'],
        'increment': increment['Value'],
        'Day': increment['Time'].day != day
        }
      day = increment['Time'].day

def val_options_table(data):
  #pprint(data[0])
  unique_rows = list(set([item['MN'] for item in data]))
  for row in unique_rows:
    cols = [{'Time': item['Time'], 'MN': row, 'Value': item['Value'], 'Header': item['SN'], 'increments': item['increments']} for item in data if item['MN'] == row]


    all_times = [x['increments'] for x in cols]
    all_times = list(set([item['Time'] for sublist in all_times for item in sublist]))

    # Because we dont save 0 values in increments, we need to add them
    # to the data that get's presented in the table
    for col in cols:
      times = [x['Time'] for x in col['increments']]
      missing_times = list(set(all_times) - set(times))
      for time in missing_times:
        col['increments'].append({ 'Time': time, 'Value': 0 })

    # Remove uneccesary data
    for col in cols:
      for increment in col['increments']:
        increment.pop('Price', None)
        increment.pop('Id', None)

    # Sort cols w.r.t time
    for col in cols:
      col['increments'].sort(key=lambda x: x['Time'])

    # Add the first element to the increments list
    for col in cols:
      tmp = dict(col)
      tmp.pop('increments')
      tmp['Increment'] = 0
      col['increments'].insert(0, tmp)

    # Merge the lists and normalize them so that all fields are present everywhere
    merged = [col['increments'] for col in cols]
    for merge in merged:
      price = merge[0]['Value']
      for item in merge[1:]:
        item['Increment'] = item['Value']
        price += round(item['Value'], 2)
        item['Value'] = round(price, 2)
        item['Header'] = merge[0]['Header']


    # Format the list into their final format
    day = merged[0][0]['Time'].day
    for i,v in enumerate(merged[0]):
      item = {}
      item['Time'] = v['Time']
      item['Day'] = v['Time'].day != day
      day = item['Time'].day
      for j,k in enumerate(merged):
        if 'MN' in k[i]:
          item['Description'] = k[i]['MN']

        item[k[i]['Header']] = {
          'Value': round(k[i]['Value'], 2),
          'Increment': k[i]['Increment'],
        }

      yield item
def tables (collection, data):
  if collection in [
    "MG431_-635082837Football", # Correct score
    "MG432_1203124766Football", # 1st half correct score
    "MG433_1273570990Football",  # 2nd half correct score
    ]:


    return list(options_table(data))

  elif collection in [
    "MG636_-48400775Football", # Winning margin
    "MG637_454400094Football",  # Winning method
    "MG749_-1148862153Football", # Team To Score + Result
    "MG267_-572852200Football", # Full Time Result + Total Goals
    "MG269_827066176Football", # Total Goals, Halves
    "MG260_-1655453786Football", # Goals
    "MG262_807991303Football", # 1st Goal + Full Time Result
    "MG266_190050240Football", # Halves Result
    "MG261_1113847840Football", # Goals + Half Result
    "MG830_782012322Football", # 2nd Half Result + 2nd Half Total Goals180.00

    ]:

    return list(val_options_table(data))

  '''
  Yes - No
  "MG428_540024986Football", # Score Draw 
  "MG236_-1953818769Football", # To Win Match With Handicap
  "MG236_439287476Football", # To Win Match With Handicap (3 way)
  "MG228_-149261393Football", # To Win Match With Asian Handicap
  '''


  return []
