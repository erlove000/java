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
    header('Content-Disposition: attachment; filename=streetlight_data_' . $formatted_to_date . '.csv'); // Update the filename here

    // Create a file pointer connected to the output stream
    $output = fopen('php://output', 'w');

    // Output the column headings
    fputcsv($output, array('District Name', 'Town Name', 'Total No Of Street Alloted', 'Total No Of Street Working', 'Total No Of Not Working', 'Total No Of Street lights Made Functional Today', 'New Complaints', 'Total Non Functional Street Points', 'Remarks', 'Added By','Date of Addition'));

    // Fetch the data based on the current user
    $sql2 = "SELECT districtid, townid FROM streetlightlogin WHERE id=" . $_SESSION['aid'];
    $results = $con->query($sql2);
    if ($results->num_rows > 0) {
        $row = mysqli_fetch_assoc($results);
        $townid = $row['townid'];
    }

    // Fetch the relevant data
    if ($townid == '0') {
        $query = mysqli_query($con, "SELECT st.*, tt.* FROM `streetlightdata` as st INNER JOIN towns_streetlight_mapping as tt ON st.town_id = tt.town_id WHERE st.DOA >= '$from_date' AND st.DOA <= '$to_date' ORDER BY tt.district_name");
    } else {
        $query = mysqli_query($con, "SELECT st.*, tt.* FROM `streetlightdata` as st INNER JOIN towns_streetlight_mapping as tt ON st.town_id = tt.town_id WHERE st.town_id = $townid AND st.DOA >= '$from_date' AND st.DOA <= '$to_date'");
    }

    // Output the data row by row
    while ($row = mysqli_fetch_assoc($query)) {
        $doa=$row['DOA'];
        $formatted_doa = date('Y-m-d', strtotime($doa));
        fputcsv($output, array(
            $row['district_name'],
            $row['town_name'],
            $row['total_street_light_alloted'],
            $row['total_working_street_light'],
            $row['total_non_working_street_light'],
            $row['total_not_working_last_24hrs'],
            $row['total_not_working_today'],
            $row['nonfunctionaltoday'],
            $row['remarks'],
            $row['Addedby'],
            $formatted_doa
        ));
    }

    // Close the file pointer
    fclose($output);
    exit();
} else {
    echo "Invalid date range.";
}
?>
