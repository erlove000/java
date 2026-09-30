<nav class="navbar col-lg-12 col-12 p-0 fixed-top d-flex flex-row">
      <div class="navbar-brand-wrapper d-flex justify-content-center">
        <div class="navbar-brand-inner-wrapper d-flex justify-content-between align-items-center w-100">
          <a class="navbar-brand brand-logo" href="dashboard.php"><strong style="color: white; font-size:14px">Street Light Monitoring </strong></a>
          <a class="navbar-brand brand-logo-mini" href="dashboard.php"><img src="images/2.svg" alt="logo"/></a>
          <button class="navbar-toggler navbar-toggler align-self-center" type="button" data-toggle="minimize">
            <span class="typcn typcn-th-menu"></span>
          </button>
        </div>
      </div>

      <div class="navbar-menu-wrapper d-flex align-items-center justify-content-end">
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