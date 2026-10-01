<?php
session_start();
include('includes/config.php');

if (strlen($_SESSION['aid']) == 0) {
  header('location:index.php');
  exit();
} else {
  // Fetch logged in user's townid
  $townid = 0;
  $sql2 = "SELECT districtid, townid FROM streetlightlogin WHERE id=" . (int)$_SESSION['aid'];
  $results = $con->query($sql2);
  if ($results && $results->num_rows > 0) {
    $row = mysqli_fetch_assoc($results);
    $townid = $row['townid'];
  }

  // Fetch Latest DOA date
  $date = date('Y-m-d');
  if ($townid == '0') {
    $query1 = "SELECT SUM(total_working_street_light) AS tottal, DOA as datedata FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
  } else {
    $query1 = "SELECT total_working_street_light as tottal, DOA as datedata FROM streetlightdata WHERE town_id = '" . $townid . "' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='" . $townid . "')";
  }
  $results = $con->query($query1);
  if ($results && $results->num_rows > 0) {
    $row = $results->fetch_assoc();
    $datetime = $row['datedata'];
    if (!empty($datetime)) {
      list($date, $time) = explode(' ', $datetime);
    }
  }

  // KPI 1: Total Street light Points (Target Value)
  $kpi_total_points = 0;
  if ($townid == '0') {
    $q_k1 = "SELECT SUM(target_value) AS target_value FROM towns_streetlight_mapping";
  } else {
    $q_k1 = "SELECT target_value FROM towns_streetlight_mapping WHERE town_id = " . $townid;
  }
  $r_k1 = $con->query($q_k1);
  if ($r_k1 && $rk1 = $r_k1->fetch_assoc()) {
    $kpi_total_points = $rk1['target_value'];
  }

  // KPI 2: Total ULBs Value Present
  $kpi_ulbs_present = 0;
  if ($townid == '0') {
    $q_k2 = "SELECT count(*) AS target_value FROM towns_streetlight_mapping WHERE target_value != '0'";
  } else {
    $q_k2 = "SELECT count(*) as target_value FROM towns_streetlight_mapping WHERE town_id = " . $townid . " LIMIT 1";
  }
  $r_k2 = $con->query($q_k2);
  if ($r_k2 && $rk2 = $r_k2->fetch_assoc()) {
    $kpi_ulbs_present = $rk2['target_value'];
  }

  // KPI 3: Towns Who Entered Data (Last Filled)
  $kpi_towns_entered = 0;
  if ($townid == '0') {
    $q_k3 = "SELECT count(DISTINCT(town_id)) AS tottalss FROM streetlightdata WHERE DOA = '" . $date . "'";
  } else {
    $q_k3 = "SELECT 1 AS tottalss FROM streetlightdata WHERE DOA = '" . $date . "' LIMIT 1";
  }
  $r_k3 = $con->query($q_k3);
  if ($r_k3 && $rk3 = $r_k3->fetch_assoc()) {
    $kpi_towns_entered = $rk3['tottalss'];
  }

  // KPI 4: Total Street lights Points Working
  $kpi_working_points = 0;
  if ($townid == '0') {
    $q_k4 = "SELECT SUM(total_working_street_light) AS tottal FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
  } else {
    $q_k4 = "SELECT total_working_street_light as tottal FROM streetlightdata WHERE town_id = '" . $townid . "' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='" . $townid . "')";
  }
  $r_k4 = $con->query($q_k4);
  if ($r_k4 && $rk4 = $r_k4->fetch_assoc()) {
    $kpi_working_points = $rk4['tottal'];
  }

  // KPI 5: Points Not Working Till Yesterday
  $kpi_not_working_yesterday = 0;
  if ($townid == '0') {
    $q_k5 = "SELECT SUM(total_non_working_street_light) AS tottalss FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
  } else {
    $q_k5 = "SELECT total_non_working_street_light as tottalss FROM streetlightdata WHERE town_id = '" . $townid . "' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='" . $townid . "')";
  }
  $r_k5 = $con->query($q_k5);
  if ($r_k5 && $rk5 = $r_k5->fetch_assoc()) {
    $kpi_not_working_yesterday = $rk5['tottalss'];
  }

  // KPI 6: Made Functional Today
  $kpi_functional_today = 0;
  if ($townid == '0') {
    $q_k6 = "SELECT SUM(total_not_working_last_24hrs) AS tottaling FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
  } else {
    $q_k6 = "SELECT total_not_working_last_24hrs AS tottaling FROM streetlightdata WHERE town_id = '" . $townid . "' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='" . $townid . "')";
  }
  $r_k6 = $con->query($q_k6);
  if ($r_k6 && $rk6 = $r_k6->fetch_assoc()) {
    $kpi_functional_today = $rk6['tottaling'];
  }

  // KPI 7: Points Not Working Today (New Complaints)
  $kpi_new_complaints = 0;
  if ($townid == '0') {
    $q_k7 = "SELECT SUM(total_not_working_today) AS complaints FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
  } else {
    $q_k7 = "SELECT total_not_working_today as complaints FROM streetlightdata WHERE town_id = '" . $townid . "' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='" . $townid . "')";
  }
  $r_k7 = $con->query($q_k7);
  if ($r_k7 && $rk7 = $r_k7->fetch_assoc()) {
    $kpi_new_complaints = $rk7['complaints'];
  }

  // KPI 8: Total Non Functional Street Points
  $kpi_non_functional_today = 0;
  if ($townid == '0') {
    $q_k8 = "SELECT SUM(nonfunctionaltoday) AS tottal FROM streetlightdata WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
  } else {
    $q_k8 = "SELECT nonfunctionaltoday as tottal FROM streetlightdata WHERE town_id = '" . $townid . "' AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='" . $townid . "')";
  }
  $r_k8 = $con->query($q_k8);
  if ($r_k8 && $rk8 = $r_k8->fetch_assoc()) {
    $kpi_non_functional_today = $rk8['tottal'];
  }
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <title>Street Light Monitoring || Dashboard</title>
  <link rel="icon" type="image/png" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
</head>

<body>
  <div class="container-scroller">
    <?php include_once('includes/header.php'); ?>

    <nav class="navbar-breadcrumb col-xl-12 col-12 d-flex flex-row p-0">
      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end w-100">
        <ul class="navbar-nav mr-lg-2">
          <li class="nav-item">
            <div class="d-flex align-items-center">
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0 font-weight-bold">Main Dashboard</p>
            </div>
          </li>
        </ul>
      </div>
    </nav>

    <div class="container-fluid page-body-wrapper">
      <?php include_once('includes/sidebar.php'); ?>

      <div class="main-panel">
        <div class="content-wrapper">

          <!-- Date Last Filled Banner Strip -->
          <div class="date-banner-strip">
            <div class="d-flex align-items-center">
              <i class="typcn typcn-calendar-outline text-primary mr-2" style="font-size: 1.4rem;"></i>
              <span class="font-weight-bold text-dark">Data Audit Date (Last Filled): <strong><?php echo htmlspecialchars($date); ?></strong></span>
            </div>
            <span class="badge badge-primary px-3 py-2" style="border-radius: 20px;"><i class="typcn typcn-refresh-outline"></i> System Active</span>
          </div>

          <!-- KPI Metric Cards Grid -->
          <div class="row">
            
            <!-- Card 1: Total Streetlights Target Points -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-blue">
                <div>
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <div class="kpi-title">Total Streetlight Points</div>
                    <div class="kpi-icon-box"><i class="typcn typcn-flash"></i></div>
                  </div>
                  <div class="kpi-value"><?php echo number_format($kpi_total_points); ?></div>
                </div>
                <div>
                  <a href="Street_lights_Points_details.php" class="kpi-footer-link">More Info <i class="typcn typcn-arrow-right-thick"></i></a>
                </div>
              </div>
            </div>

            <!-- Card 2: Total ULBs Present -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-teal">
                <div>
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <div class="kpi-title">ULBs Value Present</div>
                    <div class="kpi-icon-box"><i class="typcn typcn-briefcase"></i></div>
                  </div>
                  <div class="kpi-value"><?php echo number_format($kpi_ulbs_present); ?></div>
                </div>
                <div>
                  <span class="text-muted small">Registered ULBs</span>
                </div>
              </div>
            </div>

            <!-- Card 3: Towns Data Entered -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-indigo">
                <div>
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <div class="kpi-title">Towns Entered Data</div>
                    <div class="kpi-icon-box"><i class="typcn typcn-document-text"></i></div>
                  </div>
                  <div class="kpi-value"><?php echo number_format($kpi_towns_entered); ?></div>
                </div>
                <div>
                  <span class="text-muted small">Last Filled Submission</span>
                </div>
              </div>
            </div>

            <!-- Card 4: Working Streetlight Points -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-green">
                <div>
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <div class="kpi-title">Functional Points Working</div>
                    <div class="kpi-icon-box"><i class="typcn typcn-tick"></i></div>
                  </div>
                  <div class="kpi-value"><?php echo number_format($kpi_working_points); ?></div>
                </div>
                <div>
                  <a href="working_street.php" class="kpi-footer-link">More Info <i class="typcn typcn-arrow-right-thick"></i></a>
                </div>
              </div>
            </div>

            <!-- Card 5: Not Working Till Yesterday -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-amber">
                <div>
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <div class="kpi-title">Not Working Till Yesterday</div>
                    <div class="kpi-icon-box"><i class="typcn typcn-warning"></i></div>
                  </div>
                  <div class="kpi-value"><?php echo number_format($kpi_not_working_yesterday); ?></div>
                </div>
                <div>
                  <a href="not_working.php" class="kpi-footer-link">More Info <i class="typcn typcn-arrow-right-thick"></i></a>
                </div>
              </div>
            </div>

            <!-- Card 6: Made Functional Today -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-cyan">
                <div>
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <div class="kpi-title">Made Functional Today</div>
                    <div class="kpi-icon-box"><i class="typcn typcn-spanner"></i></div>
                  </div>
                  <div class="kpi-value"><?php echo number_format($kpi_functional_today); ?></div>
                </div>
                <div>
                  <a href="made_functional.php" class="kpi-footer-link">More Info <i class="typcn typcn-arrow-right-thick"></i></a>
                </div>
              </div>
            </div>

            <!-- Card 7: New Complaints Today -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-rose">
                <div>
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <div class="kpi-title">New Complaints Today</div>
                    <div class="kpi-icon-box"><i class="typcn typcn-times"></i></div>
                  </div>
                  <div class="kpi-value"><?php echo number_format($kpi_new_complaints); ?></div>
                </div>
                <div>
                  <a href="new_complaints.php" class="kpi-footer-link">More Info <i class="typcn typcn-arrow-right-thick"></i></a>
                </div>
              </div>
            </div>

            <!-- Card 8: Total Non Functional Street Points -->
            <div class="col-md-6 col-lg-3 grid-margin stretch-card">
              <div class="kpi-card theme-red">
                <div>
                  <div class="d-flex justify-content-between align-items-center mb-3">
                    <div class="kpi-title">Total Non-Functional</div>
                    <div class="kpi-icon-box"><i class="typcn typcn-flash-outline"></i></div>
                  </div>
                  <div class="kpi-value"><?php echo number_format($kpi_non_functional_today); ?></div>
                </div>
                <div>
                  <a href="non_functional.php" class="kpi-footer-link">More Info <i class="typcn typcn-arrow-right-thick"></i></a>
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
</body>

</html>
<?php } ?>
