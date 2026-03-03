from flask import Flask, render_template, request
import duckdb

app = Flask(__name__)
continuous_columns = ['humidity', 'temp', 'wind']
discrete_columns = ['day']
months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
days = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
sorted_months = sorted(months)

@app.route('/')
def index():
    scatter_ranges_query = f'SELECT MIN(X), MAX(X), MIN(Y), MAX(Y) FROM forestfires.csv' # Retrieves the minimum and maximum X and Y coordinates
    scatter_ranges_results = duckdb.sql(scatter_ranges_query).df()
    # scatter_ranges = [scatter_ranges_results[0], scatter_ranges_results[1], scatter_ranges_results[2], scatter_ranges_results[3]] # TODO: Define a list [minimum of X, maximum of X, minimum of Y, maximum of Y]
    scatter_ranges = scatter_ranges_results.iloc[0].to_list()  
    # print(scatter_ranges)
      
    max_count_query = '''
        SELECT MAX(month_count) 
        FROM (
            SELECT COUNT(*) AS month_count 
            FROM 'forestfires.csv' 
            GROUP BY month
        ) AS maxFFCount
    ''' # TODO: Write a query that retrieves the maximum number of forest fires that occurred in a single month
    max_count_results = duckdb.sql(max_count_query).df()
    max_count = int(max_count_results.iloc[0, 0]) # TODO: Extract the maximum count from the query results
    # print(max_count)

    # filter_ranges_query = 'SELECT * FROM forestfires.csv' # TODO: write a query that retrieves the the minimum and maximum value for each slider
    # filter_ranges_results = duckdb.sql(filter_ranges_query).df()
    humid_min_max_query = 'SELECT MIN(humidity), MAX(humidity)  FROM forestfires.csv' 
    filter_humid_ranges = duckdb.sql(humid_min_max_query).df()
    temp_min_max_query = 'SELECT MIN(temp), MAX(temp)  FROM forestfires.csv' 
    filter_temp_ranges = duckdb.sql(temp_min_max_query).df()
    wind_min_max_query = 'SELECT MIN(wind), MAX(wind) FROM forestfires.csv' 
    filter_wind_ranges = duckdb.sql(wind_min_max_query).df()
    filter_ranges = {'humidity': filter_humid_ranges.iloc[0].to_list(), 
                     'temp': filter_temp_ranges.iloc[0].to_list(), 
                     'wind': filter_wind_ranges.iloc[0].to_list()} # TODO: Create a dictionary where each key is a filter and values are the minimum and maximum values
    # print(filter_ranges)
    
    return render_template(
        'index.html', months=months, days=days,
        filter_ranges=filter_ranges, scatter_ranges=scatter_ranges, max_count=max_count
    )

@app.route('/update', methods=["POST"])
def update():
    request_data = request.get_json()
    continuous_predicate = ' AND '.join([f'({column} >= 0 AND {column} <= 0)' for column in continuous_columns]) # TODO: update where clause from sliders
    discrete_predicate = ' AND '.join([f'{column} IN ({days})' for column in discrete_columns]) # TODO: update where clause from checkboxes
    predicate = ' AND '.join([continuous_predicate, discrete_predicate]) # Combine where clause from sliders and checkboxes

    scatter_query = f'SELECT X, Y FROM forestfires.csv WHERE {predicate}'
    scatter_results = duckdb.sql(scatter_query).df()
    scatter_data = [scatter_results] # TODO: Extract the data that will populate the scatter plot
    print(scatter_data)

    bar_query = f'SELECT * FROM forestfires.csv' # TODO: Write a query that retrieves the number of forest fires per month after filtering
    bar_results = duckdb.sql(bar_query).df()
    bar_results['month'] = bar_results.index.map({i: sorted_months[i] for i in range(len(sorted_months))})
    bar_data = [] # TODO: Extract the data that will populate the bar chart from the results
    max_count = 0 # TODO: Extract the maximum number of forest fires in a single month from the results

    return {'scatter_data': scatter_data, 'bar_data': bar_data, 'max_count': max_count}

if __name__ == "__main__":
    app.run(debug=True)
    