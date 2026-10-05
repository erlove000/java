<?php
session_start();
include_once('includes/config.php');

// Fetch data from the form
$from_date = $_POST['from_date'];
$to_date = $_POST['to_date'];
$formatted_to_date = date('Y-m-d', strtotime($to_date));

// Check if the dates are set
if (isset($from_date) && isset($to_date)) {
    header('Content-Type: text/csv; charset=utf-8');
    header('Content-Disposition: attachment; filename=pbtrac_data_' . $formatted_to_date . '.csv'); // Update the filename here

    // Create a file pointer connected to the output stream
    $output = fopen('php://output', 'w');

    // Output the column headings
    fputcsv($output, array('District Name', 'ULB Name', 'Date of Reporting', 'Name of Service', 'Days Required', 'Total Applications', 'Pending Beyond Timelines', 'Added By'));

    // Fetch the data based on the current user
    $sql2 = "SELECT districtid, townid FROM streetlightlogin WHERE id=" . $_SESSION['aid'];
    $results = $con->query($sql2);
    if ($results->num_rows > 0) {
        $row = mysqli_fetch_assoc($results);
        $townid = $row['townid'];
    }

    // Fetch the relevant data from pbtrac_report
    if ($townid == '0') {
        $query = mysqli_query($con, "SELECT pr.*, tt.district_name, tt.town_name FROM `pbtrac_report` as pr 
            INNER JOIN towns_streetlight_mapping as tt ON pr.ulb = tt.town_id 
            WHERE pr.date_of_reporting >= '$from_date' AND pr.date_of_reporting <= '$to_date' 
            ORDER BY tt.district_name");
    } else {
        $query = mysqli_query($con, "SELECT pr.*, tt.district_name, tt.town_name FROM `pbtrac_report` as pr 
            INNER JOIN towns_streetlight_mapping as tt ON pr.ulb = tt.town_id 
            WHERE pr.ulb = $townid AND pr.date_of_reporting >= '$from_date' AND pr.date_of_reporting <= '$to_date'");
    }

    // Output the data row by row
    while ($row = mysqli_fetch_assoc($query)) {
        fputcsv($output, array(
            $row['district_name'],
            $row['town_name'],
            date('Y-m-d', strtotime($row['date_of_reporting'])), // Format date
            $row['name_of_service'],
            $row['days_required'],
            $row['total_applications'],
            $row['pending_beyond_timelines'],
            $row['Addedby']
        ));
    }

    // Close the file pointer
    fclose($output);
    exit();
} else {
    echo "Invalid date range.";
}
?>