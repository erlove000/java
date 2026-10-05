<nav class="navbar col-lg-12 col-12 p-0 fixed-top d-flex flex-row">
    <!-- Google Fonts -->
    <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <!-- Custom Pro UI Stylesheet -->
    <link rel="stylesheet" href="css/custom-portal.css">

      <div class="navbar-brand-wrapper d-flex justify-content-center">
        <div class="navbar-brand-inner-wrapper d-flex justify-content-between align-items-center w-100">
          <a class="navbar-brand brand-logo" href="dashboard.php">
            <img src="images/logo1.png" alt="logo" style="width:24px; height:24px; margin-right:8px; border-radius:50%; background:white; padding:2px; display:inline-block; vertical-align:middle;"/>
            <span style="display:inline-block; vertical-align:middle;">Street Light Monitoring</span>
          </a>
          <a class="navbar-brand brand-logo-mini" href="dashboard.php"><img src="images/logo1.png" alt="logo" style="width:30px; height:30px; border-radius:50%; background:white;"/></a>
        </div>
      </div>

      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end">
        
        <div class="sidebar-toggle-icon-btn" data-toggle="minimize">
          <i class="typcn typcn-th-menu"></i>
        </div>
        <ul class="navbar-nav mr-lg-2">
          <li class="nav-item nav-profile dropdown">
            <a class="nav-link" href="#" data-toggle="dropdown" id="profileDropdown">
              <img src="images/pmidc.jpg" alt="profile"/>
              <?php
$adid=$_SESSION['aid'];
$query="select staff_name from streetlightlogin where ID='$adid'";
$ret=mysqli_query($con,$query);
//echo $query;
$row=mysqli_fetch_array($ret);
$name=$row['staff_name'];

?>
              <span class="nav-profile-name"><?php echo $name; ?></span>
            </a>
            <div class="dropdown-menu dropdown-menu-right navbar-dropdown" aria-labelledby="profileDropdown">
              <a class="dropdown-item" href="profile.php">
                <i class="typcn typcn-cog-outline text-primary"></i>
                Profile
              </a>
              <a class="dropdown-item" href="change-password.php">
                <i class="typcn typcn-cog-outline text-primary"></i>
                Change Password
              </a>
              <a class="dropdown-item" href="logout.php">
                <i class="typcn typcn-eject text-primary"></i>
                Logout
              </a>
            </div>
          </li>
          <li class="nav-item nav-user-status dropdown">
            
          </li>
        </ul>
        
        <button class="navbar-toggler navbar-toggler-right d-lg-none align-self-center" type="button" data-toggle="offcanvas">
          <span class="typcn typcn-th-menu"></span>
        </button>
      </div>
    </nav><script type="text/javascript">
$(document).ready(function() {
 setInterval(runningTime, 1000);
});
function runningTime() {
  $.ajax({
    url: 'timeScript.php',
    success: function(data) {
       $('#runningTime').html(data);
     },
  });
}
</script>