<?php
session_start();
error_reporting(0);
include('includes/config.php');
if (strlen($_SESSION['aid']) == 0) {
    header('location:index.php');
    exit();
}

// Ensure Admin Access (Town 0)
$aid = (int)$_SESSION['aid'];
$admin_check = mysqli_query($con, "SELECT townid FROM streetlightlogin WHERE id=$aid");
$admin_row = mysqli_fetch_assoc($admin_check);
if ($admin_row['townid'] != 0) {
    echo "Access Denied. Admins only.";
    exit();
}

$filter_distid = isset($_GET['distid']) ? (int)$_GET['distid'] : '';

?>
<!DOCTYPE html>
<html lang="en">
<head>
    <title>ULB / Town Master || PMIDC</title>
    <link rel="stylesheet" href="vendors/typicons/typicons.css">
    <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
    <link rel="stylesheet" href="css/vertical-layout-light/style.css">
</head>
<body>
<div class="container-scroller">
    <?php include_once('includes/header.php'); ?>
    <div class="container-fluid page-body-wrapper">
        <?php include_once('includes/sidebar.php'); ?>
        <div class="main-panel">
            <div class="content-wrapper">
                <div class="row">
                    <div class="col-12 grid-margin stretch-card">
                        <div class="card shadow-sm border-0" style="border-radius: 8px;">
                            <div class="card-body">
                                <h4 class="card-title text-primary"><i class="typcn typcn-location-outline"></i> ULB / Town Master List</h4>
                                <p class="card-description">View all Town IDs and District IDs across the state.</p>
                                
                                <form method="GET" action="manage_towns.php" class="form-inline mb-4">
                                    <label class="mr-sm-2 font-weight-bold" for="distid">Filter by District: </label>
                                    <select class="form-control mb-2 mr-sm-2 p-2" name="distid" id="distid" onchange="this.form.submit()">
                                        <option value="">-- All Districts --</option>
                                        <?php
                                        // Fetch distinct districts
                                        $dist_res = mysqli_query($con, "SELECT DISTINCT distid, district_name FROM towns_streetlight_mapping ORDER BY district_name ASC");
                                        while($d = mysqli_fetch_assoc($dist_res)) {
                                            $sel = ($filter_distid == $d['distid']) ? 'selected' : '';
                                            echo "<option value='".$d['distid']."' $sel>".htmlspecialchars($d['district_name'])." (ID: ".$d['distid'].")</option>";
                                        }
                                        ?>
                                    </select>
                                    <a href="manage_towns.php" class="btn btn-outline-secondary mb-2 ml-2">Reset Filter</a>
                                </form>

                                <div class="table-responsive">
                                    <table class="table table-hover table-bordered table-striped">
                                        <thead class="bg-light">
                                            <tr>
                                                <th>#</th>
                                                <th>District ID</th>
                                                <th>District Name</th>
                                                <th>Town ID</th>
                                                <th>Town / ULB Name</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            <?php
                                            $where = "";
                                            if ($filter_distid > 0) {
                                                $where = " WHERE distid = $filter_distid ";
                                            }
                                            $towns_q = mysqli_query($con, "SELECT distid, district_name, town_id, town_name FROM towns_streetlight_mapping $where ORDER BY district_name ASC, town_name ASC");
                                            
                                            $cnt = 1;
                                            if(mysqli_num_rows($towns_q) > 0) {
                                                while($t = mysqli_fetch_assoc($towns_q)) {
                                                    echo "<tr>";
                                                    echo "<td>$cnt</td>";
                                                    echo "<td><span class='badge badge-secondary'>".$t['distid']."</span></td>";
                                                    echo "<td class='font-weight-bold'>".htmlspecialchars($t['district_name'])."</td>";
                                                    echo "<td><span class='badge badge-primary'>".$t['town_id']."</span></td>";
                                                    echo "<td>".htmlspecialchars($t['town_name'])."</td>";
                                                    echo "</tr>";
                                                    $cnt++;
                                                }
                                            } else {
                                                echo "<tr><td colspan='5' class='text-center text-muted'>No towns found for this selection.</td></tr>";
                                            }
                                            ?>
                                        </tbody>
                                    </table>
                                </div>
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
</body>
</html>
