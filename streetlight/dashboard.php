<?php session_start();
// Database Connection
include('includes/config.php');
//Validating Session
if(strlen($_SESSION['aid'])==0)
  { 
header('location:index.php');
}
else{ ?>
<!DOCTYPE html>
<html lang="en">

<head>
  
  <title>Street Light Monitoring || Dashboard</title>
  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <!-- base:css -->
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <!-- endinject -->
  
</head>
<body>
  

  <div class="container-scroller">
    <!-- partial:partials/_navbar.html -->
    <?php include_once('includes/header.php');?>
    <!-- partial -->
    <nav class="navbar-breadcrumb col-xl-12 col-12 d-flex flex-row p-0">
&nbsp;
      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end" align="right">
        <ul class="navbar-nav mr-lg-2">
          <li class="nav-item ml-0">
            <!-- <h4 class="mb-0">Dashboard</h4> -->
          </li>
          <li class="nav-item">
            <div class="d-flex align-items-baseline">
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Main Dahboard</p>
            </div>
          </li>
        </ul>
       
      </div>
    </nav>
    <div class="container-fluid page-body-wrapper">
    <?php     
                       $sql2 = "SELECT districtid, townid FROM streetlightlogin where id=".$_SESSION['aid'];
                       $results = $con->query($sql2);
                       //echo $sql2;
                       if ($results->num_rows > 0) {
                        
                         while ($row = mysqli_fetch_assoc($results)) {
                           $townid=$row['townid']; 
                         }
                       }
                       
                       if ($townid == '0') {
                        $query1 = "SELECT SUM(total_working_street_light) AS tottal ,DOA as datedata
FROM streetlightdata 
WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";

                      } else {
                        $query1 = "SELECT total_working_street_light as tottal, DOA as datedata
          FROM streetlightdata 
          WHERE town_id = '".$townid."'
          AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='".$townid."'); ";
                      }
                      #echo  $query1;
                      $results = $con->query($query1);
                      
                      // Debugging: Check if the query executed successfully
                      if (!$results) {
                          die("Query failed: " . $con->error);
                      }
                      
                      // Debugging: Print the query to ensure it's correct
                      // echo $query1;
                      
                      if ($results->num_rows > 0) {
                          while ($row = $results->fetch_assoc()) {
                              $totsccount = $row['tottal'];
                              $datetime= $row['datedata'];
                              list($date, $time) = explode(' ', $datetime);

                          }
                          // Debugging: Print the fetched result
                          // echo "Total count: " . $totsccount;
                      } 
?>
      <!-- partial:partials/_sidebar.html -->
     <?php include_once('includes/sidebar.php');?>
      <!-- partial -->
      <div class="main-panel">    <div lass="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">  <h6 style="margin-left:2.5%"><b><?php 
      echo "Showing Data of Date (Last Filled): ".$date;?><b></h6></div>
        <div class="content-wrapper">
          <div class="row"> 
            <div class="col-md-6 grid-margin stretch-card">
              <div class="card">
            
                <div class="card-body">
                  <div class="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">
                    <div>
                      <?php 

$sql2 = "SELECT districtid, townid FROM streetlightlogin where id=".$_SESSION['aid'];
$results = $con->query($sql2);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $townid=$row['townid']; 
  }
}
if ($townid == '0') {
  $query1 = "SELECT SUM(target_value) AS target_value FROM towns_streetlight_mapping";
} else {
  $query1 = "SELECT target_value FROM towns_streetlight_mapping WHERE town_id = ".$townid;
}

$results = $con->query($query1);

if ($results->num_rows > 0) {
  while ($row = $results->fetch_assoc()) {
      $totservices = $row['target_value'];
  }
} else {
  echo "No results found";

}
?>
                      <h5 class="mb-0" style="color: blue;">Total No Of Street lights Points</h5>
                      <h1 class="mb-0"><?php 
                      echo $totservices;?></h1>
                    </div>
                    <i class="typcn typcn-briefcase icon-xl text-secondary"></i>
                  </div>
                  
                  <a href="Street_lights_Points_details.php" class="small-box-footer">More info <i class="fas fa-arrow-circle-right"></i></a>
                </div>
              </div>
            </div>

<!--
            


-->



<div class="col-md-6 grid-margin stretch-card">
              <div class="card">
            
                <div class="card-body">
                  <div class="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">
                    <div>
                      <?php 

$sql2 = "SELECT districtid, townid FROM streetlightlogin where id=".$_SESSION['aid'];
$results = $con->query($sql2);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $townid=$row['townid']; 
  }
}
if ($townid == '0') {
  $query1 = "SELECT count(*) AS target_value FROM towns_streetlight_mapping where target_value != '0'";
} else {
  $query1 = "SELECT count(*) as target_value FROM towns_streetlight_mapping WHERE town_id = ".$townid." limit 1";
}

$results = $con->query($query1);

if ($results->num_rows > 0) {
  while ($row = $results->fetch_assoc()) {
      $totservices = $row['target_value'];
  }
} else {
  echo "No results found";

}
?>
                      <h5 class="mb-0" style="color: blue;">Total No Of ULB whose value present</h5>
                      <h1 class="mb-0"><?php 
                      echo $totservices;?></h1>
                    </div>
                    <i class="typcn typcn-briefcase icon-xl text-secondary"></i>
                  </div>
                  
                  <!--<a href="Street_lights_Points_details.php" class="small-box-footer">More info <i class="fas fa-arrow-circle-right"></i></a>-->
                </div>
              </div>
            </div>

            <div class="col-md-6 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <div class="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">
                    <div>
                       <?php   
                        if ($townid == '0') {
                            $query1="SELECT count(DISTINCT(town_id)) AS tottalss FROM streetlightdata WHERE DOA = '".$date."'";
                        } else {
                          $query1 = "SELECT 1 AS tottalss
FROM streetlightdata
WHERE DOA = '".$date."' 
                          LIMIT 1";
               
                       
                        }
                        
                       //echo $query1;
                       
                     ;
                      $results = $con->query($query1);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $totsccounting = $row['tottalss'];
    
  }
}
?>
                      <h5 class="mb-0" style="color: blue;"> Total No Of  Towns Who Entered Data  (Last Filled)</h5>
                      <h1 class="mb-0"><?php echo $totsccounting;?></h1>
                    </div>
                    <i class="typcn  typcn-times icon-xl text-secondary"></i>
                  </div>
                <!--  <a href="not_working.php" class="small-box-footer">More info <i class="fas fa-users-circle-right"></i></a>-->
                </div>
              </div>
            </div>












<!--
-->
            
            <div class="col-md-6 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <div class="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">
                    <div>
      
                      <h5 class="mb-0" style="color: blue;"> Total No Of Street lights Points Working   </h5> 
                      <h1 class="mb-0"><?php echo $totsccount;?></h1>
                    </div>
                                   <i class="typcn typcn-th-list icon-xl text-secondary"></i>

                  </div>
                   <a href="working_street.php" class="small-box-footer">More info <i class="fas fa-users-circle-right"></i></a>
                </div>
              </div>
            </div>
            <div class="col-md-6 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <div class="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">
                    <div>
                       <?php   
                        if ($townid == '0') {
                            $query1="SELECT SUM(total_non_working_street_light) AS tottalss 
FROM streetlightdata 
WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
                        } else {
                            $query1="SELECT total_non_working_street_light as tottalss, DOA as datedata
          FROM streetlightdata 
          WHERE town_id = '".$townid."'
          AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='".$townid."'); ";
                       
                        }
                        
                       
                       
                     ;
                      $results = $con->query($query1);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $totsccounting = $row['tottalss'];
    
  }
}
?>
                      <h5 class="mb-0" style="color: blue;"> Total No Of Street lights Points Not Working till Yesterday</h5>
                      <h1 class="mb-0"><?php echo $totsccounting;?></h1>
                    </div>
                    <i class="typcn  typcn-times icon-xl text-secondary"></i>
                  </div>
                  <a href="not_working.php" class="small-box-footer">More info <i class="fas fa-users-circle-right"></i></a>
                </div>
              </div>
            </div>
            

<!--------------------------->


<!--------------------------->
            <div class="col-md-6 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <div class="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">
                    <div>
                       <?php   
                       
                       if ($townid == '0') {
                        
                        $query1="SELECT SUM(total_not_working_last_24hrs) AS tottaling 
FROM streetlightdata 
WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
                    } else {
                       // $query1="SELECT total_not_working_last_24hrs AS tottaling FROM streetlightdata  where DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata) and town_id = ".$townid;
                  //  echo $query1;


                  $query1="SELECT total_not_working_last_24hrs AS tottaling 
          FROM streetlightdata 
          WHERE town_id = '".$townid."'
          AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='".$townid."')";
                    }
                      $results = $con->query($query1);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $tottaling = $row['tottaling'];
  }
}
?>
                      <h5 class="mb-0" style="color: blue;">Total No Of Street lights Made Functional Today</h5>
                      <h1 class="mb-0"><?php echo $tottaling;?></h1>
                    </div>
                    <i class="typcn  typcn-input-checked icon-xl text-secondary"></i>
                  </div>
                  <a href="made_functional.php" class="small-box-footer">More info <i class="fas fa-users-circle-right"></i></a>
                </div>
              </div>
            </div>

            <div class="col-md-6 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <div class="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">
                    <div>

                   
                       <?php  
                        if ($townid == '0') {
                        
                          $query1="
SELECT SUM(total_not_working_today) AS complaints 
FROM streetlightdata 
WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
                      } else {
                        $query1="SELECT total_not_working_today as complaints, DOA as datedata
          FROM streetlightdata 
          WHERE town_id = '".$townid."'
          AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='".$townid."'); ";
          
                     
                      }
                       
                       
                      $results = $con->query($query1);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $complaints = $row['complaints'];
  }
}
?>
                      <h5 class="mb-0" style="color: blue;">Total No Of Points Not Working Today(New Complaints)</h5>
                      <h1 class="mb-0"><?php echo $complaints;?></h1>
                    </div>
                    <i class="typcn typcn-th-menu-outline icon-xl text-secondary"></i>
                  </div>
                  <a href="new_complaints.php" class="small-box-footer">More info <i class="fas fa-users-circle-right"></i></a>
                </div>
              </div>
            </div>
      
            <div class="col-md-6 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <div class="d-flex align-items-center justify-content-between justify-content-md-center justify-content-xl-between flex-wrap mb-4">
                    <div>
                       <?php   
                       
                       if ($townid == '0') {
                        
                        $query1="SELECT SUM(nonfunctionaltoday) AS tottal 
FROM streetlightdata 
WHERE DATE(DOA) = (SELECT DATE(MAX(DOA)) FROM streetlightdata)";
                    } else {
                        $query1="SELECT nonfunctionaltoday as tottal, DOA as datedata
          FROM streetlightdata 
          WHERE town_id = '".$townid."'
          AND DOA = (SELECT MAX(DOA) FROM streetlightdata WHERE town_id ='".$townid."'); ";
                   
                    }
                      $results = $con->query($query1);
//echo $sql2;
if ($results->num_rows > 0) {
 
  while ($row = mysqli_fetch_assoc($results)) {
    $totsccount = $row['tottal'];
  }
}
?>
                      <h5 class="mb-0" style="color: blue;"> Total Non Functional Street Points </h5>
                      <h1 class="mb-0"><?php echo $totsccount;?></h1>
                    </div>
                    <i class="typcn typcn-th-list icon-xl text-secondary"></i>
                  </div>
                  <a href="non_functional.php" class="small-box-footer">More info <i class="fas fa-users-circle-right"></i></a>
                </div>
              </div>
            </div>


































              </div>
            </div>
          </div>
        </div>
        <!-- content-wrapper ends -->
        <!-- partial:partials/_footer.html -->
        <?php include_once('includes/footer.php');?>
        <!-- partial -->
      </div>
      <!-- main-panel ends -->
    </div>
    <!-- page-body-wrapper ends -->
  </div>
  <!-- container-scroller -->

  <!-- base:js -->
  <script src="vendors/js/vendor.bundle.base.js"></script>
  <!-- endinject -->
  <!-- Plugin js for this page-->
  <script src="vendors/chart.js/Chart.min.js"></script>
  <!-- End plugin js for this page-->
  <!-- inject:js -->
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
  <script src="js/settings.js"></script>
  <script src="js/todolist.js"></script>
  <!-- endinject -->
  <!-- Custom js for this page-->
  <script src="js/dashboard.js"></script>
  <!-- End custom js for this page-->
</body>

</html>
<?php } ?>
