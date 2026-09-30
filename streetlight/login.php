<?php
session_start();
error_reporting(0);
include("includes/config.php");
if(isset($_POST['submit']))
{
$username=$_POST['username'];
$password=md5($_POST['password']);

$ret=mysqli_query($con,"SELECT ID FROM sbmu_login WHERE UserName='$username' and Password='$password'");
echo $ret;
$num=mysqli_fetch_array($ret);
if($num>0)
{
$_SESSION['alogin']=$_POST['username'];
$_SESSION['aid']=$num['ID'];

if(!empty($_POST["remember"])) {
//COOKIES for username
setcookie ("user_login",$_POST["username"],time()+ (10 * 365 * 24 * 60 * 60));
//COOKIES for password
setcookie ("userpassword",$_POST["password"],time()+ (10 * 365 * 24 * 60 * 60));
} else {
if(isset($_COOKIE["user_login"])) {
setcookie ("user_login","");
if(isset($_COOKIE["userpassword"])) {
setcookie ("userpassword","");
        }
      }
}

header("location:dashboard.php");
}else{
echo "<script>alert('Invalid username or password');</script>";
header("location:index.php");
}
}
?>
<!DOCTYPE html>
<html lang="en">

<head>
  
  <title>SBMU|| Login Page</title>
  <!-- base:css -->  <link rel="icon" type="images/png/jpg" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
 
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <!-- endinject -->
  
</head>

<body>
  <div class="container-scroller">
    <div class="container-fluid page-body-wrapper full-page-wrapper">
      <div class="content-wrapper d-flex align-items-center auth px-0">
        <div class="row w-100 mx-0">
          <div class="col-lg-4 mx-auto">
            <div class="auth-form-light text-left py-5 px-4 px-sm-5">
              
              <h3 style="color:seagreen;">SBMU </h3>
              <img class="auth-form-light text-right px-10 px-sm-50"  style="margin-top:-90px; margin-left:70%"src="images/logo1.png" height="80px" width="80px">
              <h6 class="font-weight-light">Sign in to Swach Bharat Mission PUNJAB</h6>
              <form class="pt-3" method="post">
                <div class="form-group">
                  <input type="text" class="form-control form-control-lg border-left-0" id="username" placeholder="Username" name="username" value="<?php if(isset($_COOKIE["user_login"])) { echo $_COOKIE["user_login"]; } ?>">
                </div>
                <div class="form-group">
                  <input type="password" class="form-control form-control-lg border-left-0" id="password" placeholder="Password" name="password" required="true" value="<?php if(isset($_COOKIE["userpassword"])) { echo $_COOKIE["userpassword"]; } ?>">
                </div>
                <div class="mt-3">
                  <button type="submit" class="btn btn-block btn-info btn-lg font-weight-medium auth-form-btn" name="submit">LOGIN</button>
                </div>
                <div class="my-2 d-flex justify-content-between align-items-center">
                  <div class="form-check">
                    <label class="form-check-label text-muted">
                       <input type="checkbox" class="form-check-input" id="remember" name="remember" <?php if(isset($_COOKIE["user_login"])) { ?> checked <?php } ?>>
                      Keep me signed in
                    </label>
                  </div>
                  <a href="forgot-password.php" class="auth-link text-black">Forgot password?</a>
                </div>
               
              </form>
            </div>
          </div>
        </div>
      </div>
      <!-- content-wrapper ends -->
    </div>
    <!-- page-body-wrapper ends -->
  </div>
  <!-- container-scroller -->
  <!-- base:js -->
  <script src="vendors/js/vendor.bundle.base.js"></script>
  <!-- endinject -->
  <!-- inject:js -->
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
  <script src="js/settings.js"></script>
  <script src="js/todolist.js"></script>
  <!-- endinject -->
</body>

</html>
