<?php 
session_start();
error_reporting(0);
include_once('includes/config.php');

// Number of records per page
$records_per_page = 20; 

// Get the current page or set it to 1 if not present
if (isset($_GET['page']) && is_numeric($_GET['page'])) {
    $current_page = (int)$_GET['page'];
} else {
    $current_page = 1;
}

// Calculate the offset for the SQL query
$offset = ($current_page - 1) * $records_per_page;

// Handle search/filter
$from_date = isset($_POST['from_date']) ? $_POST['from_date'] : (isset($_GET['from_date']) ? $_GET['from_date'] : '');
$to_date = isset($_POST['to_date']) ? $_POST['to_date'] : (isset($_GET['to_date']) ? $_GET['to_date'] : '');

// Handle deletion
if (isset($_GET['del'])) {
    $serid = $_GET['id'];
    echo "<script>window.location.href='dashboard.php'</script>";
}

?>
<!DOCTYPE html>
<html lang="en">

<head>
    <title>Streetlight Details || Total Street Light Allotted</title>
    <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
    <link rel="stylesheet" href="vendors/typicons/typicons.css">
    <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
    <link rel="stylesheet" href="css/vertical-layout-light/style.css">
</head>

<body>
    <div class="container-scroller">
        <?php include_once('includes/header.php'); ?>
        <nav class="navbar-breadcrumb col-xl-12 col-12 d-flex flex-row p-0">&nbsp;</nav>
        <div class="container-fluid page-body-wrapper">
            <?php include_once('includes/sidebar.php'); ?>
            <div class="main-panel">
                <div class="content-wrapper">
                    <div class="row">
                        <div class="col-md-12">
                            <div class="card">
                                <form method="POST" action="export.php" style="overflow: auto;">
                                    <input type="hidden" name="from_date" value="<?php echo htmlentities($from_date); ?>">
                                    <input type="hidden" name="to_date" value="<?php echo htmlentities($to_date); ?>">
                                    <button type="submit" class="btn btn-primary mb-3 float-left">Export to Excel</button>
                                </form>

                                <div class="table-responsive pt-3">
                                    <table class="table table-striped project-orders-table">
                                        <thead>
                                            <tr>
                                                <th>#</th>
                                                <th>District Name</th>
                                                <th>Town Name</th>
                                                <th>Total No Of Street Allotted</th>
                                                <th>Total No Of Street Working</th>
                                                <th>Total No Of Not Working</th>
                                                <th>Total No Of Street Lights Made Functional Today</th>
                                                <th>New Complaints</th>
                                                <th>Total Non Functional Street Points</th>
                                                <th>Remarks</th>
                                                <th>Added By</th>
                                                <th>Date Of Addition</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            <?php
                                            $sql2 = "SELECT districtid, townid FROM streetlightlogin WHERE id=" . $_SESSION['aid'];
                                            $results = $con->query($sql2);

                                            if ($results->num_rows > 0) {
                                                while ($row = mysqli_fetch_assoc($results)) {
                                                    $townid = $row['townid'];
                                                }
                                            }

                                            // Fetch paginated records
                                            if ($townid == '0') {
                                                $query = mysqli_query($con, "SELECT st.*, tt.* FROM `streetlightdata` as st 
                                                INNER JOIN towns_streetlight_mapping as tt ON st.town_id=tt.town_id 
                                                WHERE st.DOA >= '$from_date' AND st.DOA <= '$to_date' 
                                                ORDER BY tt.district_name 
                                                LIMIT $offset, $records_per_page");
                                            } else {
                                                $query = mysqli_query($con, "SELECT st.*, tt.* FROM `streetlightdata` as st 
                                                INNER JOIN towns_streetlight_mapping as tt ON st.town_id=tt.town_id 
                                                WHERE st.town_id = $townid 
                                                AND st.DOA >= '$from_date' AND st.DOA <= '$to_date' 
                                                LIMIT $offset, $records_per_page");
                                            }

                                            // Count total records
                                            if ($townid == '0') {
                                                $total_query = mysqli_query($con, "SELECT COUNT(*) as total FROM `streetlightdata` as st 
                                                INNER JOIN towns_streetlight_mapping as tt ON st.town_id=tt.town_id 
                                                WHERE st.DOA >= '$from_date' AND st.DOA <= '$to_date'");
                                            } else {
                                                $total_query = mysqli_query($con, "SELECT COUNT(*) as total FROM `streetlightdata` as st 
                                                INNER JOIN towns_streetlight_mapping as tt ON st.town_id=tt.town_id 
                                                WHERE st.town_id = $townid 
                                                AND st.DOA >= '$from_date' AND st.DOA <= '$to_date'");
                                            }

                                            $total_row = mysqli_fetch_assoc($total_query);
                                            $total_records = $total_row['total'];
                                            $total_pages = ceil($total_records / $records_per_page);

                                            // Display data
                                            $cnt = 1 + $offset;
                                            while ($row = mysqli_fetch_array($query)) {
                                                $doa = $row['DOA'];
                                                $formatted_doa = date('Y-m-d', strtotime($doa));
                                            ?>
                                                <tr>
                                                    <td><?php echo htmlentities($cnt); ?></td>
                                                    <td><?php echo htmlentities($row['district_name']); ?></td>
                                                    <td><?php echo htmlentities($row['town_name']); ?></td>
                                                    <td><?php echo htmlentities($row['total_street_light_alloted']); ?></td>
                                                    <td><?php echo htmlentities($row['total_working_street_light']); ?></td>
                                                    <td><?php echo htmlentities($row['total_non_working_street_light']); ?></td>
                                                    <td><?php echo htmlentities($row['total_not_working_last_24hrs']); ?></td>
                                                    <td><?php echo htmlentities($row['total_not_working_today']); ?></td>
                                                    <td><?php echo htmlentities($row['nonfunctionaltoday']); ?></td>
                                                    <td><?php echo htmlentities($row['remarks']); ?></td>
                                                    <td><?php echo htmlentities($row['Addedby']); ?></td>
                                                    <td><?php echo htmlentities($formatted_doa); ?></td>
                                                </tr>
                                            <?php $cnt = $cnt + 1; } ?>
                                        </tbody>
                                    </table>
                                </div>
                                
                                <!-- Pagination Controls -->
                                <div class="pagination-controls">
                                    <nav aria-label="Page navigation">
                                        <ul class="pagination justify-content-center">
                                            <?php if ($current_page > 1): ?>
                                                <li class="page-item">
                                                    <a class="page-link" href="?page=<?php echo $current_page - 1; ?>&from_date=<?php echo htmlentities($from_date); ?>&to_date=<?php echo htmlentities($to_date); ?>" aria-label="Previous">
                                                        <span aria-hidden="true">&laquo;</span>
                                                    </a>
                                                </li>
                                            <?php endif; ?>

                                            <?php for ($i = 1; $i <= $total_pages; $i++): ?>
                                                <li class="page-item <?php if ($i == $current_page) echo 'active'; ?>">
                                                    <a class="page-link" href="?page=<?php echo $i; ?>&from_date=<?php echo htmlentities($from_date); ?>&to_date=<?php echo htmlentities($to_date); ?>"><?php echo $i; ?></a>
                                                </li>
                                            <?php endfor; ?>

                                            <?php if ($current_page < $total_pages): ?>
                                                <li class="page-item">
                                                    <a class="page-link" href="?page=<?php echo $current_page + 1; ?>&from_date=<?php echo htmlentities($from_date); ?>&to_date=<?php echo htmlentities($to_date); ?>" aria-label="Next">
                                                        <span aria-hidden="true">&raquo;</span>
                                                    </a>
                                                </li>
                                            <?php endif; ?>
                                        </ul>
                                    </nav>
                                </div>
                                
                            </div>
                        </div>
                    </div>
                </div>
                <?php include_once('includes/footer.php'); ?>
            </div>
        </div>
    </div>
    
    <script src="vendors/js/vendor.bundle.base.js"></script>
    <script src="vendors/chart.js/Chart.min.js"></script>
    <script src="js/off-canvas.js"></script>
    <script src="js/hoverable-collapse.js"></script>
    <script src="js/template.js"></script>
    <script src="js/settings.js"></script>
    <script src="js/todolist.js"></script>
    <script src="js/dashboard.js"></script>
</body>

</html>
