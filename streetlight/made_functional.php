<?php session_start();
error_reporting(0);
include_once('includes/config.php');

if($_GET['del']){
$serid=$_GET['id'];
mysqli_query($con,"delete from tblservices where ID ='$serid'");
echo "<script>alert('Data Deleted');</script>";
echo "<script>window.location.href='manage-services.php'</script>";
          }
?>
<!DOCTYPE html>
<html lang="en">

<head>
  
  <title>Total No Of Street lights Made Functional Today
</title>  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
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
      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end">
        <ul class="navbar-nav mr-lg-2">
          <li class="nav-item ml-0">
            <!-- <h4 class="mb-0">Total No Of Street lights Made Functional Today
            </h4> -->
          </li>
          <li class="nav-item">
            <div class="d-flex align-items-baseline">
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Total No Of Street lights Made Functional Today
              </p>
            </div>
          </li>
        </ul>
       
      </div>
    </nav>
    <div class="container-fluid page-body-wrapper">
  
      <!-- partial:partials/_sidebar.html -->
     <?php include_once('includes/sidebar.php');?>
      <!-- partial -->
      <div class="main-panel">
        <div class="content-wrapper">


          <div class="row">
            <div class="col-md-12">
              <div class="card">
                <h4 class="card-title" style="padding-left: 20px; padding-top: 20px;">Total No Of Street lights Made Functional Today</h4>
                  <p class="card-description" style="padding-left: 20px;"> 
                  </p>
                <div class="table-responsive pt-3">
                  
                  <table class="table table-striped project-orders-table">
                    <thead>
                      <tr>
                        <th class="ml-5">Serial Number</th>
                        <th>District Name</th>
                        <th>Town Name</th>
                        <th>Total No Of Street lights Made Functional Today
                        </th>
                        <th>Date of addition</th>
                        <th>Addedby</th>

                      </tr>
                    </thead>
                    <tbody>
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
                         $query=mysqli_query($con,"select tt.town_name,tt.district_name as district_name,sd.total_not_working_last_24hrs,DATE(sd.DOA) AS DOA,sd.Addedby from streetlightdata as sd inner join towns_streetlight_mapping as tt on sd.town_id=tt.town_id order by sd.DOA desc;");
                      }else{
                        $query=mysqli_query($con,"select tt.town_name,tt.district_name as district_name,sd.total_not_working_last_24hrs,DATE(sd.DOA) AS DOA,sd.Addedby from streetlightdata as sd inner join towns_streetlight_mapping as tt on sd.town_id=tt.town_id  WHERE  sd.town_id = '".$townid."'ORDER by sd.DOA desc");

                      }
$cnt=1;
while($row=mysqli_fetch_array($query))
{
?>
                      <tr>

                        <td><?php echo htmlentities($cnt);?></td>
                        <td><?php echo htmlentities($row['district_name']);?> </td>
                        <td><?php echo htmlentities($row['town_name']);?></td>
                        <td><?php echo htmlentities($row['total_not_working_last_24hrs']);?> </td>
                        <td><?php echo htmlentities($row['DOA']);?> </td>
                        <td><?php echo htmlentities($row['Addedby']);?> </td>
                      
                        
                        <td>
                          <!-- <div class="d-flex align-items-center">
                            <a href="edit-services.php?id=<?php echo $row['release_id']?>" class="btn btn-success btn-sm btn-icon-text mr-3">View <i class="typcn typcn-eye btn-icon-append"></i> </a> 
                                         
                          </div> -->
                        </td>
                      </tr><?php $cnt=$cnt+1; } ?>
                      
                    </tbody>
                  </table>


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

