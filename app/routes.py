from typing import Optional
from fastapi import APIRouter, HTTPException
from app.services import get_table_info
from app.db import execute_query
from fastapi import Query
from datetime import datetime, timedelta

router = APIRouter()

@router.get("/tables_summary_single_date")
def tables_summary_single_date(
    source: Optional[str] = Query(None, description="Filter by source"),
    date: Optional[str] = Query(None, description="Single date in YYYY-MM-DD format"),
):
    """
    Retrieve tables summary for a single date, including total tables,
    tables not extracted, successful extractions, and failed extractions.
    """
    try:
        # Get database and table info dynamically
        table_info = get_table_info("db2")

        # Get current date if date is not provided
        current_date = datetime.now().strftime('%Y-%m-%d')
        if date is None:
            date = current_date

        # Construct the query filters for source and date
        source_filter = ""
        total_tables_filter = ""

        if source and source != "all":
            # Filter for a specific source
            source_filter = f"AND source = '{source}'"
            total_tables_filter = f"WHERE source = '{source}'"
        else:
            # For 'all', include all sources for the given date
            source_filter = f"AND DATE(extractedtime) = '{date}'"

        # Query to get all distinct tables
        total_tables_query = f"""
        SELECT DISTINCT source, tablename 
        FROM {table_info['database']}.{table_info['table']}
        {total_tables_filter}
        """
        total_tables_data = execute_query(total_tables_query)
        total_tables_list = [
            {"source": row[0], "tablename": row[1]} for row in total_tables_data
        ]
        total_tables_set = {(row[0], row[1]) for row in total_tables_data}

        # Query for successful extractions
        success_query = f"""
        SELECT 
            source,
            tablename,
            MAX(DATE_FORMAT(extractedtime, '%H:%i:%s')) AS latest_time,
            status
        FROM {table_info['database']}.{table_info['table']}
        WHERE DATE(extractedtime) = '{date}'
        {source_filter}
        AND status = 'success'
        GROUP BY source, tablename, status
        """

        # Query for failed extractions
        failed_query = f"""
        SELECT 
            source,
            tablename,
            MAX(DATE_FORMAT(extractedtime, '%H:%i:%s')) AS latest_time,
            status,
            status_message
        FROM {table_info['database']}.{table_info['table']}
        WHERE DATE(extractedtime) = '{date}'
        {source_filter}
        AND status != 'success'
        GROUP BY source, tablename, status, status_message
        """

        # Execute queries
        success_data = execute_query(success_query)
        failed_data = execute_query(failed_query)

        # Process successful extractions
        success_result = [
            {
                "date": date,
                "time": row[2],  # latest_time
                "source": row[0],
                "tablename": row[1],
                "status": "success"
            }
            for row in success_data
        ]
        success_tables_set = {(row[0], row[1]) for row in success_data}  # (source, tablename)

        # Process failed extractions
        failed_result = [
            {
                "date": date,
                "time": row[2],  # latest_time
                "source": row[0],
                "tablename": row[1],
                "status": row[3],
                "status_message": row[4],
            }
            for row in failed_data
        ]
        failed_tables_set = {(row[0], row[1]) for row in failed_data}  # (source, tablename)

        # Calculate tables not extracted
        processed_tables_set = success_tables_set | failed_tables_set
        not_extracted_tables_set = total_tables_set - processed_tables_set
        not_extracted_list = [
            {"source": source, "tablename": tablename}
            for source, tablename in not_extracted_tables_set
        ]

        # Return the results as a JSON response
        return {
            "status": "success",
            "total_tables": {
                "count": len(total_tables_list),
                "data": total_tables_list
            },
            "tables_not_extracted": {
                "count": len(not_extracted_list),
                "data": not_extracted_list
            },
            "successful_extractions": {
                "total_records": len(success_result),
                "data": success_result
            },
            "failed_extractions": {
                "total_records": len(failed_result),
                "data": failed_result
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables_summary_date_range")
def tables_summary_date_range(
    source: Optional[str] = Query(None, description="Filter by source"),
    from_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    to_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
):
    """
    Retrieve tables summary including total tables, 
    tables not extracted, successful extractions, and failed extractions.
    """
    try:
        # Get database and table info dynamically
        table_info = get_table_info("db2")

        # Get current date if from_date or to_date is not provided
        current_date = datetime.now().strftime('%Y-%m-%d')
        if from_date is None:
            from_date = current_date
        if to_date is None:
            to_date = current_date

        # Construct the query filters for source and date range
        source_filter = ""
        total_tables_filter = f"WHERE DATE(extractedtime) BETWEEN '{from_date}' AND '{to_date}'"

        if source and source != "all":
            source_filter = f"AND source = '{source}'"
            total_tables_filter += f" AND source = '{source}'"

        # Query to get all distinct tables for the source (or all sources if source is 'all')
        total_tables_query = f"""
        SELECT DISTINCT source, tablename 
        FROM {table_info['database']}.{table_info['table']}
        {total_tables_filter}
        """
        total_tables_data = execute_query(total_tables_query)
        total_tables_list = [
            {"source": row[0], "tablename": row[1]} for row in total_tables_data
        ]
        total_tables_set = {(row[0], row[1]) for row in total_tables_data}

        # Construct the base query for successful extractions with latest time for each date
        success_query = f"""
        SELECT 
            DATE(extractedtime) AS extraction_date,
            source,
            tablename,
            MAX(DATE_FORMAT(extractedtime, '%H:%i:%s')) AS latest_time,
            status
        FROM {table_info['database']}.{table_info['table']}
        WHERE DATE(extractedtime) BETWEEN '{from_date}' AND '{to_date}'
        {source_filter}
        AND status = 'success'
        GROUP BY extraction_date, source, tablename, status
        """
        
        # Construct the base query for failed extractions
        failed_query = f"""
        SELECT 
            DATE(extractedtime) AS extraction_date,
            source,
            tablename,
            MAX(DATE_FORMAT(extractedtime, '%H:%i:%s')) AS latest_time,
            status,
            status_message
        FROM {table_info['database']}.{table_info['table']}
        WHERE DATE(extractedtime) BETWEEN '{from_date}' AND '{to_date}'
        {source_filter}
        AND status != 'success'
        GROUP BY extraction_date, source, tablename, status, status_message
        """

        # Execute queries
        success_data = execute_query(success_query)  # Query for successful extractions
        failed_data = execute_query(failed_query)  # Query for failed extractions

        # Convert successful extractions into structured response
        success_result = [
            {
                "date": row[0],  # extraction_date from the query
                "time": row[3],  # latest_time from the query
                "source": row[1],
                "tablename": row[2],
                "status": "success"
            }
            for row in success_data
        ]

        # Convert failed extractions into structured response
        failed_result = [
            {
                "date": row[0],  # extraction_date from the query
                "time": row[3],  # latest_time from the query
                "source": row[1],
                "tablename": row[2],
                "status": row[4],  # status for failed extraction
                "status_message": row[5],
            }
            for row in failed_data
        ]

        # Calculate distinct successfully extracted tables
        success_tables_set = {(entry["source"], entry["tablename"]) for entry in success_result}

        # Calculate tables not extracted (difference between total and successful)
        not_extracted_tables_set = total_tables_set - success_tables_set
        not_extracted_list = [{"source": source, "tablename": tablename} for source, tablename in not_extracted_tables_set]

        # Return the results as a JSON response
        return {
            "status": "success",
            "total_tables": {
                "count": len(total_tables_list),
                "data": total_tables_list
            },
            "tables_not_extracted": {
                "count": len(not_extracted_list),
                "data": not_extracted_list
            },
            "successful_extractions": {
                "total_records": len(success_result),
                "data": success_result
            },
            "failed_extractions": {
                "total_records": len(failed_result),
                "data": failed_result
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/summary_counts")
def summary_counts(
    date: Optional[str] = Query(None, description="Single date in YYYY-MM-DD format"),
    source: Optional[str] = Query(None, description="Filter by source")
):
    """
    Retrieve total counts for various metrics including extracted, inserted, open counts, storage, etc.
    """
    try:
        # Retrieve table info dynamically
        table_info = get_table_info("db1_db2")

        # Get current date if no date is provided
        current_date = datetime.now().strftime('%Y-%m-%d')
        if date is None:
            date = current_date

        # Base filter conditions for table2 (for db2)
        table2_filters = []
        if source and source != "all":
            table2_filters.append(f"source = '{source}'")
        table2_filters.append(f"DATE(extractedtime) = '{date}'")
        table2_filter_condition = " AND ".join(table2_filters) if table2_filters else "1=1"

        # Base filter conditions for table1 (for db1)
        table1_filters = []
        if source and source != "all":
            table1_filters.append(f"source = '{source}'")
        table1_filters.append(f"DATE(EodMarker) = '{date}'")
        table1_filter_condition = " AND ".join(table1_filters) if table1_filters else "1=1"

        # Queries for total extracted and inserted counts (use db2.table2)
        total_extracted_query = f"""
            SELECT SUM(extractedreccount) AS total_extracted
            FROM {table_info['database_2']}.{table_info['table_2']}
            WHERE {table2_filter_condition}
        """
        total_inserted_query = f"""
            SELECT SUM(insertedreccount) AS total_inserted
            FROM {table_info['database_2']}.{table_info['table_2']}
            WHERE {table2_filter_condition}
        """

        # Queries for additional counts (use db1.table1)
        counts_queries = {
            "total_insert_open": f"SELECT SUM(InsertOpen) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_updated_open": f"SELECT SUM(UpdateOpen) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_all_storage": f"SELECT SUM(AllStorage) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_delete_non_open": f"SELECT SUM(DeletesNonOpen) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_open": f"SELECT SUM(Open) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_non_open": f"SELECT SUM(NonOpen) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_storage_duplicates": f"SELECT SUM(StorageDuplicates) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_duplicates": f"SELECT SUM(DiffenDuplicates) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}"
        }

        # Execute queries and collect results
        total_extracted = execute_query(total_extracted_query)[0][0] or 0
        total_inserted = execute_query(total_inserted_query)[0][0] or 0

        other_counts = {}
        for key, query in counts_queries.items():
            other_counts[key] = execute_query(query)[0][0] or 0

        # Prepare the response
        response = {
            "total_extracted": total_extracted,
            "total_inserted": total_inserted,
            **other_counts
        }

        return {"status": "success", "data": response}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary_counts_date_range")
def summary_counts_date_range(
    from_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    to_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    source: Optional[str] = Query(None, description="Filter by source")
):
    """
    Retrieve total counts for various metrics including extracted, inserted, open counts, storage, etc.
    """
    try:
        # Retrieve table info dynamically
        table_info = get_table_info("db1_db2")

        # Get current date if no dates are provided
        current_date = datetime.now().strftime('%Y-%m-%d')

        if from_date is None and to_date is None:
            from_date = to_date = current_date
        elif from_date is None:
            from_date = to_date
        elif to_date is None:
            to_date = from_date

        # Validate date formats
        try:
            datetime.strptime(from_date, '%Y-%m-%d')
            datetime.strptime(to_date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

        # Base filter conditions for table2 (for db2)
        table2_filters = []
        if source and source != "all":
            table2_filters.append(f"source = '{source}'")
        table2_filters.append(f"DATE(extractedtime) BETWEEN '{from_date}' AND '{to_date}'")
        table2_filter_condition = " AND ".join(table2_filters) if table2_filters else "1=1"

        # Base filter conditions for table1 (for db1)
        table1_filters = []
        if source and source != "all":
            table1_filters.append(f"source = '{source}'")
        table1_filters.append(f"DATE(EodMarker) BETWEEN '{from_date}' AND '{to_date}'")
        table1_filter_condition = " AND ".join(table1_filters) if table1_filters else "1=1"

        # Queries for total extracted and inserted counts (use db2.table2)
        total_extracted_query = f"""
            SELECT SUM(extractedreccount) AS total_extracted
            FROM {table_info['database_2']}.{table_info['table_2']}
            WHERE {table2_filter_condition}
        """
        total_inserted_query = f"""
            SELECT SUM(insertedreccount) AS total_inserted
            FROM {table_info['database_2']}.{table_info['table_2']}
            WHERE {table2_filter_condition}
        """

        # Queries for additional counts (use db1.table1)
        counts_queries = {
            "total_insert_open": f"SELECT SUM(InsertOpen) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_updated_open": f"SELECT SUM(UpdateOpen) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_all_storage": f"SELECT SUM(AllStorage) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_delete_non_open": f"SELECT SUM(DeletesNonOpen) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_open": f"SELECT SUM(Open) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_non_open": f"SELECT SUM(NonOpen) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_storage_duplicates": f"SELECT SUM(StorageDuplicates) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}",
            "total_duplicates": f"SELECT SUM(DiffenDuplicates) FROM {table_info['database_1']}.{table_info['table_1']} WHERE {table1_filter_condition}"
        }

        # Execute queries and collect results
        total_extracted = execute_query(total_extracted_query)[0][0] or 0
        total_inserted = execute_query(total_inserted_query)[0][0] or 0

        other_counts = {}
        for key, query in counts_queries.items():
            other_counts[key] = execute_query(query)[0][0] or 0

        # Prepare the response
        response = {
            "total_extracted": total_extracted,
            "total_inserted": total_inserted,
            **other_counts
        }

        return {"status": "success", "data": response}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/inserted_record_counts")
def inserted_record_counts(
    date_range: str,  # daily, weekly, monthly, yearly
    source: Optional[str] = Query(None, description="Filter by source"),
    date: str = Query(..., description="Date for filtering (format: YYYY-MM-DD)")
):
    try:
        # Get database and table info dynamically
        table_info = get_table_info("db2")
        
        # Build filters based on source if provided
        filters = []
        if source and source != "all":
            filters.append(f"source = '{source}'")
        
        # Convert the provided date string to a datetime object
        input_date = datetime.strptime(date, '%Y-%m-%d')
        
        # Initialize the query
        query = ""
        
        if date_range == 'daily':
            # For the daily range
            filters.append(f"DATE(extractedtime) = '{date}'")
            query = f"""
                SELECT HOUR(extractedtime) AS hour, SUM(insertedreccount) AS InsertedCount
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY HOUR(extractedtime)
                ORDER BY hour
            """
        
        elif date_range == 'weekly':
            # For the weekly range
            week_start_date = input_date - timedelta(days=input_date.weekday())  # Get the start of the week
            week_end_date = week_start_date + timedelta(days=8)  # Get the end of the week
            filters.append(f"DATE(extractedtime) BETWEEN '{week_start_date.date()}' AND '{week_end_date.date()}'")
            query = f"""
                SELECT DATE(extractedtime) AS date, SUM(insertedreccount) AS InsertedCount
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY DATE(extractedtime)
                ORDER BY date
            """
        
        elif date_range == 'monthly':
            # For the monthly range
            month_start_date = input_date.replace(day=1)
            next_month = input_date.replace(day=28) + timedelta(days=4)  # Go to the next month
            month_end_date = next_month - timedelta(days=next_month.day)
            filters.append(f"DATE(extractedtime) BETWEEN '{month_start_date.date()}' AND '{month_end_date.date()}'")
            query = f"""
                SELECT DATE(extractedtime) AS date, SUM(insertedreccount) AS InsertedCount
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY DATE(extractedtime)
                ORDER BY date
            """
        
        elif date_range == 'yearly':
            year_start_date = input_date.replace(month=1, day=1)
            year_end_date = input_date.replace(month=12, day=31)
            filters.append(f"DATE(extractedtime) BETWEEN '{year_start_date.date()}' AND '{year_end_date.date()}'")
            query = f"""
                SELECT MONTH(extractedtime) AS month, SUM(insertedreccount) AS InsertedCount
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY MONTH(extractedtime)
                ORDER BY month
            """
        
        else:
            raise HTTPException(status_code=400, detail="Invalid date_range. Choose from 'daily', 'weekly', 'monthly', or 'yearly'.")
        
        # Execute the query
        result = execute_query(query)
        
        # For daily data, ensure we return hourly data (0-23)
        if date_range == "daily":
            hourly_data = {i: 0 for i in range(24)}  # Initialize with zeros for all 24 hours
            
            # Fill in the result data into the hourly_data dictionary
            for row in result:
                hourly_data[row[0]] = row[1]  # Populate the result into corresponding hour
            
            # Format the response data
            response_data = [{"hour": hour, "insertedreccount": count} for hour, count in hourly_data.items()]
            return {"status": "success", "data": response_data}
        
        # For weekly data, ensure we return all days of the week (e.g., Monday-Sunday)
        if date_range == "weekly":
            # Get all days in the current week (start from Sunday to Saturday)
            week_days = [(week_start_date + timedelta(days=i)).date() for i in range(7)]
            week_data = {day: 0 for day in week_days}  # Initialize with zeros for all days
            
            # Map the returned dates to corresponding days of the week
            for row in result:
                date = row[0]
                total_all_storage = row[1]
                week_data[date] = total_all_storage
            
            # Format the response data
            response_data = [{"date": day, "TotalAllStorage": count} for day, count in week_data.items()]
            return {"status": "success", "data": response_data}
        
        # For monthly data, ensure we return all days of the month (1-31)
        if date_range == "monthly":
            # Get the total number of days in the month
            total_days_in_month = (month_end_date - month_start_date).days + 1
            days_of_month = [f"day{day}" for day in range(1, total_days_in_month + 1)]
            month_data = {day: 0 for day in days_of_month}  # Initialize with zeros for all days
            
            # Map the returned dates to corresponding day labels
            for row in result:
                date = row[0]
                inserted_count = row[1]
                day_of_month = date.day
                month_data[days_of_month[day_of_month - 1]] = inserted_count
            
            # Format the response data
            response_data = [{"day": day, "insertedreccount": count} for day, count in month_data.items()]
            return {"status": "success", "data": response_data}
        
        # For yearly data, ensure we return data for all 12 months (January to December)
        if date_range == "yearly":
            # Initialize the result data with 0 for all months
            monthly_data = {i: 0 for i in range(1, 13)}  # Initialize for months 1 to 12
            
            # Fill in the result data into the monthly_data dictionary
            for row in result:
                monthly_data[row[0]] = row[1]  # Populate the result into corresponding month
            
            # Format the response data
            response_data = [{"month": month, "insertedreccount": count} for month, count in monthly_data.items()]
            return {"status": "success", "data": response_data}
        
        # Return other date range data (daily, weekly, yearly)
        return {"status": "success", "data": result}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/inserted_counts_by_date_range")
def inserted_counts_by_date_range(
    from_date: str = Query(..., description="Start date for filtering (format: YYYY-MM-DD)"),
    to_date: str = Query(..., description="End date for filtering (format: YYYY-MM-DD)"),
    source: Optional[str] = Query(None, description="Filter by source")
):
    """
    Fetch record counts for a specified date range.
    """
    try:
        # Validate the date formats
        start_date = datetime.strptime(from_date, '%Y-%m-%d')
        end_date = datetime.strptime(to_date, '%Y-%m-%d')
        
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="from_date cannot be after to_date.")
        
        # Get dynamic table and database info
        table_info = get_table_info("db2")
        
        # Build the base query
        filters = [f"DATE(extractedtime) BETWEEN '{from_date}' AND '{to_date}'"]
        
        if source and source != "all":
            filters.append(f"source = '{source}'")
        
        query = f"""
            SELECT DATE(extractedtime) AS date, SUM(insertedreccount) AS InsertedCount
            FROM {table_info['database']}.{table_info['table']}
            WHERE {' AND '.join(filters)}
            GROUP BY DATE(extractedtime)
            ORDER BY date
        """
        
        # Execute the query
        result = execute_query(query)
        
        # Prepare response data
        response_data = [{"date": row[0], "insertedreccount": row[1]} for row in result]
        return {"status": "success", "data": response_data}
    
    except ValueError as e:
        # Handle invalid date format
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/allstorage_counts")
def allstorage_counts(
    date_range: str,  # daily, weekly, monthly, yearly
    source: Optional[str] = Query(None, description="Filter by source"),
    date: str = Query(..., description="Date for filtering (format: YYYY-MM-DD)")
):
    try:
        # Get database and table info dynamically
        table_info = get_table_info("db1")
        
        # Build filters based on source if provided
        filters = []
        if source and source != "all":
            filters.append(f"source = '{source}'")
        
        # Convert the provided date string to a datetime object
        input_date = datetime.strptime(date, '%Y-%m-%d')
        
        # Initialize the query
        query = ""
        
        if date_range == 'daily':
            # For the daily range
            filters.append(f"DATE_FORMAT(EodMarker, '%Y-%m-%d') = '{date}'")  # Compare full date (without time)
            query = f"""
                SELECT HOUR(EodMarker) AS hour, SUM(AllStorage) AS TotalAllStorage
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY HOUR(EodMarker)
                ORDER BY hour
            """
        
        elif date_range == 'weekly':
            # For the weekly range
            week_start_date = input_date - timedelta(days=input_date.weekday())  # Get the start of the week
            week_end_date = week_start_date + timedelta(days=6)  # Get the end of the week
            filters.append(f"DATE(EodMarker) BETWEEN '{week_start_date.date()}' AND '{week_end_date.date()}'")  # Use EodMarker
            query = f"""
                SELECT DATE(EodMarker) AS date, SUM(AllStorage) AS TotalAllStorage
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY DATE(EodMarker)
                ORDER BY date
            """
        
        elif date_range == 'monthly':
            # For the monthly range
            month_start_date = input_date.replace(day=1)
            next_month = input_date.replace(day=28) + timedelta(days=4)  # Go to the next month
            month_end_date = next_month - timedelta(days=next_month.day)
            filters.append(f"DATE(EodMarker) BETWEEN '{month_start_date.date()}' AND '{month_end_date.date()}'")  # Use EodMarker
            query = f"""
                SELECT DATE(EodMarker) AS date, SUM(AllStorage) AS TotalAllStorage
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY DATE(EodMarker)
                ORDER BY date
            """
        
        elif date_range == 'yearly':
            year_start_date = input_date.replace(month=1, day=1)
            year_end_date = input_date.replace(month=12, day=31)
            filters.append(f"DATE(EodMarker) BETWEEN '{year_start_date.date()}' AND '{year_end_date.date()}'")  # Use EodMarker
            query = f"""
                SELECT MONTH(EodMarker) AS month, SUM(AllStorage) AS TotalAllStorage
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY MONTH(EodMarker)
                ORDER BY month
            """
        
        else:
            raise HTTPException(status_code=400, detail="Invalid date_range. Choose from 'daily', 'weekly', 'monthly', or 'yearly'.")
        
        # Execute the query
        result = execute_query(query)
        
        # For daily data, ensure we return hourly data (0-23)
        if date_range == "daily":
            hourly_data = {i: 0 for i in range(24)}  # Initialize with zeros for all 24 hours
            
            # Fill in the result data into the hourly_data dictionary
            for row in result:
                hourly_data[row[0]] = row[1]  # Populate the result into corresponding hour
            
            # Format the response data
            response_data = [{"hour": hour, "TotalAllStorage": count} for hour, count in hourly_data.items()]
            return {"status": "success", "data": response_data}
        
        # For weekly data, ensure we return all days of the week (e.g., Monday-Sunday)
        if date_range == "weekly":
            # Get all days in the current week (start from Sunday to Saturday)
            week_days = [(week_start_date + timedelta(days=i)).date() for i in range(7)]
            week_data = {day: 0 for day in week_days}  # Initialize with zeros for all days
            
            # Map the returned dates to corresponding days of the week
            for row in result:
                date = row[0]
                total_all_storage = row[1]
                week_data[date] = total_all_storage
            
            # Format the response data
            response_data = [{"date": day, "TotalAllStorage": count} for day, count in week_data.items()]
            return {"status": "success", "data": response_data}
        
        # For monthly data, ensure we return all days of the month (1-31)
        if date_range == "monthly":
            # Get the total number of days in the month
            total_days_in_month = (month_end_date - month_start_date).days + 1
            days_of_month = [f"day{day}" for day in range(1, total_days_in_month + 1)]
            month_data = {day: 0 for day in days_of_month}  # Initialize with zeros for all days
            
            # Map the returned dates to corresponding day labels
            for row in result:
                date = row[0]
                total_all_storage = row[1]
                day_of_month = date.day
                month_data[days_of_month[day_of_month - 1]] = total_all_storage
            
            # Format the response data
            response_data = [{"day": day, "TotalAllStorage": count} for day, count in month_data.items()]
            return {"status": "success", "data": response_data}
        
        # For yearly data, ensure we return data for all 12 months (January to December)
        if date_range == "yearly":
            # Initialize the result data with 0 for all months
            monthly_data = {i: 0 for i in range(1, 13)}  # Initialize for months 1 to 12
            
            # Fill in the result data into the monthly_data dictionary
            for row in result:
                monthly_data[row[0]] = row[1]  # Populate the result into corresponding month
            
            # Format the response data
            response_data = [{"month": month, "TotalAllStorage": count} for month, count in monthly_data.items()]
            return {"status": "success", "data": response_data}
        
        # Return other date range data (daily, weekly, yearly)
        return {"status": "success", "data": result}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/allstorage_date_range")
def allstorage_date_range(
    from_date: str = Query(..., description="Start date for filtering data (format: YYYY-MM-DD)"),
    to_date: str = Query(..., description="End date for filtering data (format: YYYY-MM-DD)"),
    source: Optional[str] = Query(None, description="Optional filter by source")
):
    try:
        # Convert the provided date strings to datetime objects
        start_date = datetime.strptime(from_date, '%Y-%m-%d')
        end_date = datetime.strptime(to_date, '%Y-%m-%d')
        
        # Ensure the start date is not after the end date
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="Start date cannot be after end date")
        
        # Get database and table info dynamically
        table_info = get_table_info("db1")

        # Build filters based on source if provided
        filters = [f"DATE(EodMarker) BETWEEN '{start_date.date()}' AND '{end_date.date()}'"]
        if source and source != "all":
            filters.append(f"source = '{source}'")

        # Construct the SQL query to sum the AllStorage by date
        query = f"""
            SELECT DATE(EodMarker) AS date, SUM(AllStorage) AS allstorage_count
            FROM {table_info['database']}.{table_info['table']}
            WHERE {' AND '.join(filters)}
            GROUP BY DATE(EodMarker)
            ORDER BY DATE(EodMarker)
        """

        # Execute the query
        result = execute_query(query)
        
        # Format the response data
        response_data = [{"date": row[0].strftime('%Y-%m-%d'), "allstorage_count": row[1]} for row in result]
        
        return {"status": "success", "data": response_data}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/open_non_open_counts")
def open_non_open_counts(
    date_range: str,  # "daily", "weekly", "monthly", "yearly"
    source: Optional[str] = Query(None, description="Filter by source"),
    date: str = Query(..., description="Date for filtering (format: YYYY-MM-DD)")
):
    try:
        # Get database and table info
        table_info = get_table_info("db1")
        
        # Convert input date to a datetime object
        input_date = datetime.strptime(date, "%Y-%m-%d")
        
        # Build the query filters
        filters = []
        if source and source != "all":
            filters.append(f"source = '{source}'")
        
        query = ""

        # Define SQL query based on the `date_range`
        if date_range == "daily":
            filters.append(f"DATE(EodMarker) = '{date}'")
            query = f"""
                SELECT DATE(EodMarker) AS date, HOUR(EodMarker) AS hour, 
                       SUM(Open) AS OpenCount, SUM(NonOpen) AS NonOpenCount
                FROM {table_info['database']}.{table_info['table']}
                WHERE {' AND '.join(filters)}
                GROUP BY DATE(EodMarker), HOUR(EodMarker)
                ORDER BY date, hour
            """
        
        elif date_range == "weekly":
            # Calculate 7 days starting from the input date
            week_start = input_date
            week_end = input_date + timedelta(days=6)
            filters.append(f"DATE(EodMarker) BETWEEN '{week_start.date()}' AND '{week_end.date()}'")
            query = f"""
                WITH date_series AS (
                    SELECT DATE('{week_start.date()}' + INTERVAL (t.n - 1) DAY) AS date
                    FROM (
                        SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                        FROM information_schema.columns
                        LIMIT 7
                    ) t
                )
                SELECT 
                    ds.date, 
                    COALESCE(SUM(c.Open), 0) AS OpenCount, 
                    COALESCE(SUM(c.NonOpen), 0) AS NonOpenCount
                FROM 
                    date_series ds
                LEFT JOIN 
                    {table_info['database']}.{table_info['table']} c 
                    ON DATE(c.EodMarker) = ds.date 
                    AND {' AND '.join(filters.replace("EodMarker", "c.EodMarker") for filters in filters)}
                GROUP BY 
                    ds.date
                ORDER BY 
                    ds.date
            """
        
        elif date_range == "monthly":
            # Calculate 30 days from the input date
            month_start = input_date
            month_end = input_date + timedelta(days=29)
            filters.append(f"DATE(EodMarker) BETWEEN '{month_start.date()}' AND '{month_end.date()}'")
            query = f"""
                WITH date_series AS (
                    SELECT DATE('{month_start.date()}' + INTERVAL (t.n - 1) DAY) AS date
                    FROM (
                        SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
                        FROM information_schema.columns
                        LIMIT 30
                    ) t
                )
                SELECT 
                    ds.date, 
                    COALESCE(SUM(c.Open), 0) AS OpenCount, 
                    COALESCE(SUM(c.NonOpen), 0) AS NonOpenCount
                FROM 
                    date_series ds
                LEFT JOIN 
                    {table_info['database']}.{table_info['table']} c 
                    ON DATE(c.EodMarker) = ds.date 
                    AND {' AND '.join(filters.replace("EodMarker", "c.EodMarker") for filters in filters)}
                GROUP BY 
                    ds.date
                ORDER BY 
                    ds.date
            """
        
        elif date_range == "yearly":
            # Calculate the start and end of the year
            year_start = input_date.replace(month=1, day=1)  # Start of the year
            year_end = input_date.replace(month=12, day=31)  # End of the year

            # Add date filter for the year range
            filters.append(f"DATE(EodMarker) BETWEEN '{year_start.date()}' AND '{year_end.date()}'")
            filters_sql = " AND ".join(filters)

            # Generate the SQL query
            query = f"""
            WITH RECURSIVE month_series AS (
                SELECT '{year_start.date()}' AS month
                UNION ALL
                SELECT month + INTERVAL 1 MONTH
                FROM month_series
                WHERE month < '{year_end.date()}'
            )
            SELECT 
                DATE_FORMAT(ms.month, '%Y-%m') AS month, 
                COALESCE(SUM(c.Open), 0) AS OpenCount, 
                COALESCE(SUM(c.NonOpen), 0) AS NonOpenCount
            FROM 
                month_series ms
            LEFT JOIN 
                {table_info['database']}.{table_info['table']} c 
                ON YEAR(c.EodMarker) = YEAR(ms.month) 
                AND MONTH(c.EodMarker) = MONTH(ms.month)
            WHERE 
                {filters_sql}
            GROUP BY 
                ms.month
            ORDER BY 
                ms.month;
            """

        
        else:
            raise HTTPException(status_code=400, detail="Invalid date_range. Choose from 'daily', 'weekly', 'monthly', or 'yearly'.")
        
        # Execute the query
        result = execute_query(query)

        # Format the response for different date ranges
        if date_range == "daily":
            response_data = [{"date": row[0], "hour": row[1], "OpenCount": row[2], "NonOpenCount": row[3]} for row in result]
            for hour in range(24):
                if not any(d['hour'] == hour for d in response_data):
                    response_data.append({
                        "date": date,
                        "hour": hour,
                        "OpenCount": 0,
                        "NonOpenCount": 0
                    })
            response_data.sort(key=lambda x: x["hour"])  # Sort by hour
        elif date_range == "weekly":
            response_data = [{"date": row[0], "OpenCount": row[1], "NonOpenCount": row[2]} for row in result]
        elif date_range == "monthly":
            response_data = [{"date": row[0], "OpenCount": row[1], "NonOpenCount": row[2]} for row in result]
        else:  # yearly
            response_data = [{"month": row[0], "OpenCount": row[1], "NonOpenCount": row[2]} for row in result]
        
        # Return the response
        return {"status": "success", "data": response_data}
    
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/open_non_open_counts_by_date_range")
def open_non_open_counts_by_date_range(
    from_date: str,  # Start date in format YYYY-MM-DD
    to_date: str,  # End date in format YYYY-MM-DD
    source: Optional[str] = Query(None, description="Filter by source")
):
    try:
        # Convert input dates to datetime objects
        start_date = datetime.strptime(from_date, "%Y-%m-%d")
        end_date = datetime.strptime(to_date, "%Y-%m-%d")
        
        # Get database and table info
        table_info = get_table_info("db1")
        
        # Build the query filters
        filters = []
        if source and source != "all":
            filters.append(f"source = '{source}'")
        
        # Add date range filter
        filters.append(f"DATE(EodMarker) BETWEEN '{start_date.date()}' AND '{end_date.date()}'")
        
        # Create SQL query to aggregate counts by date
        query = f"""
            SELECT DATE(EodMarker) AS date,
                   SUM(Open) AS open_count,
                   SUM(NonOpen) AS non_open_count
            FROM {table_info['database']}.{table_info['table']}
            WHERE {' AND '.join(filters)}
            GROUP BY DATE(EodMarker)
            ORDER BY date;
        """
        
        # Execute the query
        result = execute_query(query)

        # Format the response
        response_data = [
            {"date": row[0], "open_count": row[1], "non_open_count": row[2]} for row in result
        ]

        # Return the response
        return {"status": "success", "data": response_data}
    
    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data_breakdown")
def data_breakdown(
    source: Optional[str] = Query(None, description="Filter by source"),
    date: Optional[str] = Query(None, description="Base date for analysis in YYYY-MM-DD format"),
    breakdown_type: Optional[str] = Query(None, description="Type of breakdown: daily, weekly, monthly, yearly")
):
    try:
        # Get database and table info dynamically
        table_info = get_table_info("db1")
        
        # Build base filters
        filters = []
        if source and source != "all":
            filters.append(f"source = '{source}'")
        
        # Validate and parse the date
        if date:
            try:
                base_date = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        else:
            base_date = datetime.now()
        
        # Prepare query based on breakdown type
        if breakdown_type == "daily":
            # Single percentage for the specific day
            query = f"""
                SELECT 
                    COALESCE(SUM(Open), 0) as total_open, 
                    COALESCE(SUM(NonOpen), 0) as total_non_open, 
                    COALESCE(SUM(StorageDuplicates), 0) as total_duplicates
                FROM {table_info['database']}.{table_info['table']}
                WHERE DATE(EodMarker) = '{base_date.strftime('%Y-%m-%d')}'
                {' AND ' + ' AND '.join(filters) if filters else ''}  # Handle filters for source
            """
        elif breakdown_type == "weekly":
            # Single percentage for 7 days starting from the given date
            query = f"""
                SELECT 
                    COALESCE(SUM(Open), 0) as total_open, 
                    COALESCE(SUM(NonOpen), 0) as total_non_open, 
                    COALESCE(SUM(StorageDuplicates), 0) as total_duplicates
                FROM {table_info['database']}.{table_info['table']}
                WHERE EodMarker BETWEEN 
                    '{base_date.strftime('%Y-%m-%d')}' 
                    AND '{(base_date + timedelta(days=6)).strftime('%Y-%m-%d')}'
                {' AND ' + ' AND '.join(filters) if filters else ''}  # Handle filters for source
            """
        elif breakdown_type == "monthly":
            # Single percentage for the entire month of the given date
            query = f"""
                SELECT 
                    COALESCE(SUM(Open), 0) as total_open, 
                    COALESCE(SUM(NonOpen), 0) as total_non_open, 
                    COALESCE(SUM(StorageDuplicates), 0) as total_duplicates
                FROM {table_info['database']}.{table_info['table']}
                WHERE 
                    YEAR(EodMarker) = {base_date.year} 
                    AND MONTH(EodMarker) = {base_date.month}
                {' AND ' + ' AND '.join(filters) if filters else ''}  # Handle filters for source
            """
        elif breakdown_type == "yearly":
            # Single percentage for the entire year specified in the input date
            query = f"""
                SELECT 
                    COALESCE(SUM(Open), 0) as total_open, 
                    COALESCE(SUM(NonOpen), 0) as total_non_open, 
                    COALESCE(SUM(StorageDuplicates), 0) as total_duplicates
                FROM {table_info['database']}.{table_info['table']}
                WHERE 
                    YEAR(EodMarker) = {base_date.year}
                {' AND ' + ' AND '.join(filters) if filters else ''}  # Handle filters for source
            """
        else:
            raise HTTPException(status_code=400, detail="Invalid breakdown type")
        
        # Execute the query
        results = execute_query(query)
        
        # Ensure we have results
        if not results:
            return {"status": "success", "data": {"Open": 0, "Non Open": 0, "Duplicates": 0}}
        
        # Calculate total and percentages
        total_open, total_non_open, total_duplicates = results[0]
        
        # Handle case where all values might be zero
        total = total_open + total_non_open + total_duplicates
        
        # Calculate percentages
        response_data = {
            "Open": (total_open / total * 100) if total else 0,
            "Non Open": (total_non_open / total * 100) if total else 0,
            "Duplicates": (total_duplicates / total * 100) if total else 0
        }
        
        return {"status": "success", "data": response_data}
    
    except Exception as e:
        # Log the full error for debugging
        print(f"Error in data_breakdown: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/data_by_date_range_percentage")
def data_by_date_range_percentage(
    from_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    to_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    source: Optional[str] = Query(None, description="Filter by source")
):
    try:
        # Validate and parse the from_date and to_date
        try:
            start_date = datetime.strptime(from_date, "%Y-%m-%d")
            end_date = datetime.strptime(to_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
        
        # Ensure that from_date is before to_date
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="from_date cannot be later than to_date.")
        
        # Get database and table info dynamically
        table_info = get_table_info("db1")

        # Build filters based on the provided source
        filters = []
        if source and source != "all":
            filters.append(f"source = '{source}'")
        
        # Prepare the query to fetch data for each day in the specified date range
        query = f"""
            SELECT 
                DATE(EodMarker) as date,
                COALESCE(SUM(Open), 0) as total_open, 
                COALESCE(SUM(NonOpen), 0) as total_non_open, 
                COALESCE(SUM(StorageDuplicates), 0) as total_duplicates
            FROM {table_info['database']}.{table_info['table']}
            WHERE EodMarker BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
            {' AND ' + ' AND '.join(filters) if filters else ''}  # Handle filters for source
            GROUP BY DATE(EodMarker)
            ORDER BY DATE(EodMarker)
        """
        
        # Execute the query
        results = execute_query(query)

        # Ensure we have results
        if not results:
            return {
                "status": "success",
                "data": {
                    "from_date": from_date,
                    "to_date": to_date,
                    "data": []
                }
            }

        # Prepare the data for each date with percentages
        data = []
        for result in results:
            date, total_open, total_non_open, total_duplicates = result
            total = total_open + total_non_open + total_duplicates

            # Avoid division by zero and calculate percentages
            if total > 0:
                open_percentage = (total_open / total) * 100
                non_open_percentage = (total_non_open / total) * 100
                duplicates_percentage = (total_duplicates / total) * 100
            else:
                open_percentage = non_open_percentage = duplicates_percentage = 0

            # Add the date and calculated percentages to the data list
            data.append({
                "date": str(date),
                "Open": open_percentage,
                "Non Open": non_open_percentage,
                "Duplicates": duplicates_percentage
            })
        
        # Return the data with date range and daily percentages
        return {
            "status": "success",
            "data": {
                "from_date": from_date,
                "to_date": to_date,
                "data": data
            }
        }
    
    except Exception as e:
        # Log the full error for debugging
        print(f"Error in data_by_date_range_percentage: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
