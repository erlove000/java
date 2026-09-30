<!-- <?php session_start();
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
  
  <title>SBMU|| URINALS</title>  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
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
            <h4 class="mb-0">URINALS</h4>
          </li>
          <li class="nav-item">
            <div class="d-flex align-items-baseline">
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">URINALS Services</p>
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
                <h4 class="card-title" style="padding-left: 20px; padding-top: 20px;">URINALS Services</h4>
                  <p class="card-description" style="padding-left: 20px;"> 
                  </p>
                <div class="table-responsive pt-3">
                  
                  <table class="table table-striped project-orders-table">
                    <thead>
                      <tr>
                        <th class="ml-5">Serial Number</th>
                        <th>Release  Id</th>
                        <th>Released Seats</th>
                        <th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      <?php
                       

                         $query=mysqli_query($con,"select * from sbmu_release_data where release_cat='urinals' ");
$cnt=1;
while($row=mysqli_fetch_array($query))
{
?>
                      <tr>

                        <td><?php echo htmlentities($cnt);?></td>
                        <td><?php echo htmlentities($row['release_id']);?> </td>
                        <td><?php echo htmlentities($row['release_seats']);?></td>
                      
                        
                        <td>
                          <div class="d-flex align-items-center">
                            <a href="edit-services.php?id=<?php echo $row['release_id']?>" class="btn btn-success btn-sm btn-icon-text mr-3">View <i class="typcn typcn-eye btn-icon-append"></i> </a> 
                                         
                          </div>
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
 -->
