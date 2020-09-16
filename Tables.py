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



def tables (collection, data):
  if collection == "MG431_-635082837Football":
    return list(options_table(data))

  return []
