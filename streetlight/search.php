<?php session_start();
error_reporting(0);
// Database Connection
include('includes/config.php');
//Validating Session
if(strlen($_SESSION['aid'])==0)
  { 
header('location:login.php');
}
else{


  ?>
<!DOCTYPE html>
<html lang="en">

<head>
  
  <title>SBMU 2.0. || Search Details</title>
  <!-- base:css -->  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
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
            <h4 class="mb-0">Search  Details</h4>
          </li>
          <li class="nav-item">
            <div class="d-flex align-items-baseline">
              <p class="mb-0">Home</p>
              <i class="typcn typcn-chevron-right"></i>
              <p class="mb-0">Search  Details</p>
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
            <form class="forms-sample" method="post">
                    
                    
                   
                  </form>

                  <div class="content-wrapper">
          <div class="row">
            <div class="col-md-12 grid-margin stretch-card">
              <div class="card">
                <div class="card-body">
                  <h4 class="card-title">Search</h4>
              
                  <form class="forms-sample" method="post">
                    
                   <div class="form-group">
                      <label for="exampleInputUsername1">Search By Submitted ID</label>
                      <input type="text" id="searchdata" name="searchdata" class="form-control" required="required" autofocus="autofocus" >
                    </div>
                   
                   
                    <button type="submit" name="search" class="btn btn-info btn-min-width mr-1 mb-1">Search</button>
                  </form>
                </div>
              </div>
            </div>
     
          </div>
        </div>
            <div class="col-md-12">

              <div class="card">
 

                 
                <div class="table-responsive pt-3">
                  
                  <?php
if(isset($_POST['search']))
{ 

$sdata=$_POST['searchdata'];
  ?>
                <h4 class="card-title" style="padding-left: 20px; padding-top: 20px;">Result against "<?php echo $sdata;?>" keyword</h4>
                  <table class="table table-striped project-orders-table">
                    <thead>
                      <tr>
                        <th class="ml-5">#</th>
                        <th>District Name</th>
                        <th>Town Name</th>
                        <th>Release Category</th>
                        <th>Release Scheme</th>
                        <th>Release seats</th>
                        <th>Release Date</th>
                        <th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      <?php
                         $query=mysqli_query($con,"select s.*,d.district_name,t.town_name  from sbmu_release_data s, district d, sbmu_towns t where s.release_id='$sdata.'
                         and d.district_id=s.release_dist
                         and t.town_id=s.release_town");
                         $num=mysqli_num_rows($query);
if($num>0){
$cnt=1;
while($row=mysqli_fetch_array($query))
{
?>
                       <tr>
                        <td><?php echo htmlentities($row['release_id']);?></td>
                        <td><?php echo htmlentities($row['district_name']);?> </td>
                        <td><?php echo htmlentities($row['town_name']);?> </td>
                        <td><?php echo htmlentities($row['release_cat']);?> </td>
                        <td><?php echo htmlentities($row['release_scheme']);?> </td>
                        <td><?php echo htmlentities($row['release_seats']);?> </td>
                        <td><?php echo htmlentities($row['release_date']);?></td>
                        <td>
                          <div class="d-flex align-items-center">
                            <a href="table.php?id=<?php echo $row['release_id']?>" class="btn btn-success btn-sm btn-icon-text mr-3">View <i class="typcn typcn-eye btn-icon-append"></i> </a> 
                                            
                          </div>
                        </td>
                      </tr> <?php 
$cnt=$cnt+1;
} } else { ?>
  <tr>
    <td colspan="8"> No record found against this search</td>

  </tr>
   
<?php } }?>
                      
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

</html><?php } ?>

