<?php session_start();
// Database Connection
include('includes/config.php');
//Validating Session
if(strlen($_SESSION['aid'])==0) { 
  header('location:index.php');
  exit();
}

$sql2 = "SELECT districtid, townid FROM streetlightlogin where id=".(int)$_SESSION['aid'];
$results = $con->query($sql2);
$townid = '0';
if ($results->num_rows > 0) {
  while ($row = mysqli_fetch_assoc($results)) {
    $townid = $row['townid']; 
  }
}

$date = '';
$time = '';
$totsccount_working = 0;
if ($townid == '0') {
  $query1 = "SELECT SUM(total_working_street_light) AS tottal ,DOA as datedata FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
} else {
  $query1 = "SELECT total_working_street_light as tottal, DOA as datedata FROM streetlightdata WHERE town_id = '".$townid."' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='".$townid."');";
}
$res1 = $con->query($query1);
if ($res1 && $res1->num_rows > 0) {
    while ($row = $res1->fetch_assoc()) {
        $totsccount_working = $row['tottal'];
        $datetime = $row['datedata'];
        if($datetime) {
            list($date, $time) = explode(' ', $datetime);
        }
    }
}
?>
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Street Light Monitoring || Dashboard</title>
  <link rel="icon" type="image/png" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
</head>
<body>
  <div class="container-scroller">
    <?php include_once('includes/header.php');?>
    
    <nav class="navbar-breadcrumb col-xl-12 col-12 d-flex flex-row p-0">
      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end" align="right">
        <ul class="navbar-nav mr-lg-2">
          <li class="nav-item">
            <div class="d-flex align-items-baseline">
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Main Dashboard</p>
            </div>
          </li>
        </ul>
      </div>
    </nav>
    
    <div class="container-fluid page-body-wrapper">
      <?php include_once('includes/sidebar.php');?>
      
      <div class="main-panel">
        <div class="content-wrapper">
          
          <div class="date-banner-strip">
            <div class="d-flex align-items-center">
              <i class="typcn typcn-calendar-outline text-primary mr-2" style="font-size: 1.5rem;"></i>
              <span class="font-weight-bold mr-2 text-dark">Data Audit Date (Last Filled):</span> 
              <strong><?php echo htmlspecialchars($date);?></strong>
            </div>
            <div>
              <span class="badge" style="background:#8b5cf6; color:white; padding:8px 16px; border-radius:20px; font-weight:700;"><i class="typcn typcn-flash"></i> System Active</span>
            </div>
          </div>

          <div class="row"> 
            
            <!-- Card 1 -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-blue">
                <div>
                  <div class="d-flex justify-content-between align-items-start mb-3">
                    <h5 class="kpi-title">Total Streetlight Points</h5>
                    <div class="kpi-icon-box"><i class="typcn typcn-lightbulb"></i></div>
                  </div>
                  <?php 
                  if ($townid == '0') { $q = "SELECT SUM(target_value) AS tv FROM towns_streetlight_mapping"; } 
                  else { $q = "SELECT target_value AS tv FROM towns_streetlight_mapping WHERE town_id = $townid"; }
                  $res = $con->query($q);
                  $val = ($res && $res->num_rows>0) ? $res->fetch_assoc()['tv'] : 0;
                  ?>
                  <h1 class="kpi-value"><?php echo number_format((int)$val);?></h1>
                </div>
                <a href="Street_lights_Points_details.php" class="kpi-footer-link">More Info <i class="fa-solid fa-arrow-right"></i></a>
              </div>
            </div>

            <!-- Card 2 -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-teal">
                <div>
                  <div class="d-flex justify-content-between align-items-start mb-3">
                    <h5 class="kpi-title">ULBs Value Present</h5>
                    <div class="kpi-icon-box"><i class="typcn typcn-briefcase"></i></div>
                  </div>
                  <?php 
                  if ($townid == '0') { $q = "SELECT count(*) AS tv FROM towns_streetlight_mapping where target_value != '0'"; } 
                  else { $q = "SELECT count(*) AS tv FROM towns_streetlight_mapping WHERE town_id = $townid LIMIT 1"; }
                  $res = $con->query($q);
                  $val = ($res && $res->num_rows>0) ? $res->fetch_assoc()['tv'] : 0;
                  ?>
                  <h1 class="kpi-value"><?php echo number_format((int)$val);?></h1>
                </div>
                <a href="#" class="kpi-footer-link">More Info <i class="fa-solid fa-arrow-right"></i></a>
              </div>
            </div>

            <!-- Card 3 -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-indigo">
                <div>
                  <div class="d-flex justify-content-between align-items-start mb-3">
                    <h5 class="kpi-title">Towns Entered Data</h5>
                    <div class="kpi-icon-box"><i class="typcn typcn-document-text"></i></div>
                  </div>
                  <?php 
                  if ($townid == '0') { $q = "SELECT count(DISTINCT(town_id)) AS tv FROM streetlightdata WHERE DOA = '$date'"; } 
                  else { $q = "SELECT 1 AS tv FROM streetlightdata WHERE DOA = '$date' LIMIT 1"; }
                  $res = $con->query($q);
                  $val = ($res && $res->num_rows>0) ? $res->fetch_assoc()['tv'] : 0;
                  ?>
                  <h1 class="kpi-value"><?php echo number_format((int)$val);?></h1>
                </div>
                <a href="#" class="kpi-footer-link">More Info <i class="fa-solid fa-arrow-right"></i></a>
              </div>
            </div>

            <!-- Card 4 -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-green">
                <div>
                  <div class="d-flex justify-content-between align-items-start mb-3">
                    <h5 class="kpi-title">Functional Points Working</h5>
                    <div class="kpi-icon-box"><i class="typcn typcn-tick-outline"></i></div>
                  </div>
                  <h1 class="kpi-value"><?php echo number_format((int)$totsccount_working);?></h1>
                </div>
                <a href="working_street.php" class="kpi-footer-link">More Info <i class="fa-solid fa-arrow-right"></i></a>
              </div>
            </div>

            <!-- Card 5 -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card mt-4">
              <div class="kpi-card theme-amber">
                <div>
                  <div class="d-flex justify-content-between align-items-start mb-3">
                    <h5 class="kpi-title">Not Working Till Yesterday</h5>
                    <div class="kpi-icon-box"><i class="typcn typcn-warning-outline"></i></div>
                  </div>
                  <?php 
                  if ($townid == '0') { $q = "SELECT SUM(total_non_working_street_light) AS tv FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)"; } 
                  else { $q = "SELECT total_non_working_street_light AS tv FROM streetlightdata WHERE town_id = '$townid' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='$townid')"; }
                  $res = $con->query($q);
                  $val = ($res && $res->num_rows>0) ? $res->fetch_assoc()['tv'] : 0;
                  ?>
                  <h1 class="kpi-value"><?php echo number_format((int)$val);?></h1>
                </div>
                <a href="not_working.php" class="kpi-footer-link">More Info <i class="fa-solid fa-arrow-right"></i></a>
              </div>
            </div>
            
            <!-- Card 6 -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card mt-4">
              <div class="kpi-card theme-cyan">
                <div>
                  <div class="d-flex justify-content-between align-items-start mb-3">
                    <h5 class="kpi-title">Made Functional Today</h5>
                    <div class="kpi-icon-box"><i class="typcn typcn-spanner-outline"></i></div>
                  </div>
                  <?php 
                  if ($townid == '0') { $q = "SELECT SUM(total_not_working_last_24hrs) AS tv FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)"; } 
                  else { $q = "SELECT total_not_working_last_24hrs AS tv FROM streetlightdata WHERE town_id = '$townid' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='$townid')"; }
                  $res = $con->query($q);
                  $val = ($res && $res->num_rows>0) ? $res->fetch_assoc()['tv'] : 0;
                  ?>
                  <h1 class="kpi-value"><?php echo number_format((int)$val);?></h1>
                </div>
                <a href="made_functional.php" class="kpi-footer-link">More Info <i class="fa-solid fa-arrow-right"></i></a>
              </div>
            </div>

            <!-- Card 7 -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card mt-4">
              <div class="kpi-card theme-rose">
                <div>
                  <div class="d-flex justify-content-between align-items-start mb-3">
                    <h5 class="kpi-title">New Complaints Today</h5>
                    <div class="kpi-icon-box"><i class="typcn typcn-times"></i></div>
                  </div>
                  <?php 
                  if ($townid == '0') { $q = "SELECT SUM(total_not_working_today) AS tv FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)"; } 
                  else { $q = "SELECT total_not_working_today AS tv FROM streetlightdata WHERE town_id = '$townid' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='$townid')"; }
                  $res = $con->query($q);
                  $val = ($res && $res->num_rows>0) ? $res->fetch_assoc()['tv'] : 0;
                  ?>
                  <h1 class="kpi-value"><?php echo number_format((int)$val);?></h1>
                </div>
                <a href="new_complaints.php" class="kpi-footer-link">More Info <i class="fa-solid fa-arrow-right"></i></a>
              </div>
            </div>

            <!-- Card 8 -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card mt-4">
              <div class="kpi-card theme-red">
                <div>
                  <div class="d-flex justify-content-between align-items-start mb-3">
                    <h5 class="kpi-title">Total Non-Functional</h5>
                    <div class="kpi-icon-box"><i class="typcn typcn-flash-outline"></i></div>
                  </div>
                  <?php 
                  if ($townid == '0') { $q = "SELECT SUM(nonfunctionaltoday) AS tv FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)"; } 
                  else { $q = "SELECT nonfunctionaltoday AS tv FROM streetlightdata WHERE town_id = '$townid' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='$townid')"; }
                  $res = $con->query($q);
                  $val = ($res && $res->num_rows>0) ? $res->fetch_assoc()['tv'] : 0;
                  ?>
                  <h1 class="kpi-value"><?php echo number_format((int)$val);?></h1>
                </div>
                <a href="non_functional.php" class="kpi-footer-link">More Info <i class="fa-solid fa-arrow-right"></i></a>
              </div>
            </div>

          </div>
        </div>
        <!-- content-wrapper ends -->
        
        <?php include_once('includes/footer.php');?>
      </div>
      <!-- main-panel ends -->
    </div>
    <!-- page-body-wrapper ends -->
  </div>

  <script src="vendors/js/vendor.bundle.base.js"></script>
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
  <script src="js/settings.js"></script>
  <script src="js/todolist.js"></script>
</body>
</html>
