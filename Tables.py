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
  if collection in ["MG431_-635082837Football", "MG432_1203124766Football", "MG433_1273570990Football"]:
    '''
    ADD
    Correct score
    1st half correct score
    2nd half correct score

    Maybe:
      1st half + Full time
      1st half + 2nd Half
    '''

    return list(options_table(data))

  elif collection in ["MG636_-48400775Football"]:
    '''
    ADD
    Winning margin
    Winning method

    Maybe:
      Score Draw
      Half with most goals
      Half with First goal

    '''
    return list(val_options_table(data))

  return []
