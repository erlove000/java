<?php
session_start();
error_reporting(0);
include('includes/config.php');

$msg = "";
$error = "";

if (isset($_POST['submit'])) {
  $contactno = $_SESSION['contactno'];
  $email = $_SESSION['email'];
  $password = md5($_POST['newpassword']);

  if (empty($email) || empty($contactno)) {
    $error = "Session expired. Please start the password reset process again.";
  } else {
    $query = mysqli_query($con, "UPDATE streetlightlogin SET Password='$password' WHERE Email='$email' AND mobile_no='$contactno'");
    if ($query) {
      session_destroy();
      echo "<script>
        alert('Password successfully updated! Please sign in with your new password.');
        window.location.href = 'index.php';
      </script>";
      exit();
    } else {
      $error = "Failed to update password: " . mysqli_error($con);
    }
  }
}
?>
<!DOCTYPE html>
<html lang="en">

<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
  <title>Set New Password || PMIDC Street Light Monitoring System</title>
  <link rel="icon" type="image/png" href="images/pmidc.jpg" />
  <link rel="stylesheet" href="vendors/typicons/typicons.css">
  <link rel="stylesheet" href="vendors/css/vendor.bundle.base.css">
  <link rel="stylesheet" href="css/vertical-layout-light/style.css">
  <style>
    html, body {
      height: 100%;
      margin: 0;
      font-family: 'Manrope', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background-color: #ffffff;
    }
    
    .login-container {
      min-height: 100vh;
      width: 100%;
      display: flex;
      margin: 0;
      padding: 0;
    }

    /* Left Hero Column - 58% Width */
    .hero-side {
      flex: 0 0 58%;
      max-width: 58%;
      background: linear-gradient(135deg, #0b1329 0%, #1e293b 45%, #0284c7 100%);
      color: #ffffff;
      padding: 60px 80px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      position: relative;
      overflow: hidden;
    }

    .hero-side::before {
      content: "";
      position: absolute;
      top: -100px;
      right: -100px;
      width: 450px;
      height: 450px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(2, 132, 199, 0.22) 0%, rgba(0,0,0,0) 70%);
      pointer-events: none;
    }

    .brand-header-logos {
      display: flex;
      align-items: center;
      gap: 15px;
    }

    .brand-logo-card {
      background: #ffffff;
      padding: 8px 14px;
      border-radius: 12px;
      box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
      display: flex;
      align-items: center;
    }

    .brand-logo-card img {
      height: 48px;
      width: auto;
    }

    .hero-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: rgba(255, 255, 255, 0.12);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(255, 255, 255, 0.2);
      color: #e0f2fe;
      padding: 6px 16px;
      border-radius: 30px;
      font-size: 0.85rem;
      font-weight: 600;
      letter-spacing: 0.5px;
      margin-top: 35px;
      margin-bottom: 20px;
    }

    .hero-title {
      font-size: 2.5rem;
      font-weight: 800;
      line-height: 1.25;
      color: #ffffff;
      margin-bottom: 12px;
    }

    .hero-subtitle {
      font-size: 1.05rem;
      color: #94a3b8;
      max-width: 540px;
      line-height: 1.6;
    }

    /* Feature Glass Cards */
    .feature-cards-grid {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 20px;
      margin-top: 40px;
    }

    .feature-card {
      background: rgba(255, 255, 255, 0.07);
      backdrop-filter: blur(12px);
      border: 1px solid rgba(255, 255, 255, 0.12);
      border-radius: 16px;
      padding: 22px;
      transition: transform 0.3s ease, background 0.3s ease;
    }

    .feature-icon-wrapper {
      width: 44px;
      height: 44px;
      border-radius: 12px;
      background: linear-gradient(135deg, #38bdf8 0%, #0284c7 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.4rem;
      color: #ffffff;
      margin-bottom: 14px;
      box-shadow: 0 8px 16px rgba(2, 132, 199, 0.3);
    }

    .feature-card h5 {
      font-size: 1.05rem;
      font-weight: 700;
      color: #ffffff;
      margin-bottom: 6px;
    }

    .feature-card p {
      font-size: 0.85rem;
      color: #cbd5e1;
      margin: 0;
      line-height: 1.5;
    }

    .hero-footer-text {
      font-size: 0.85rem;
      color: #64748b;
      margin: 0;
    }

    /* Right Form Column - 42% Width */
    .form-side {
      flex: 0 0 42%;
      max-width: 42%;
      background: #ffffff;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      padding: 50px 60px;
    }

    .form-wrapper {
      width: 100%;
      max-width: 440px;
    }

    .form-header h2 {
      font-size: 2rem;
      font-weight: 800;
      color: #0f172a;
      margin-bottom: 6px;
    }

    .form-header p {
      font-size: 0.95rem;
      color: #64748b;
      margin-bottom: 32px;
    }

    .custom-input-group {
      position: relative;
      margin-bottom: 22px;
    }

    .custom-input-group label {
      display: block;
      font-size: 0.82rem;
      font-weight: 700;
      color: #334155;
      letter-spacing: 0.5px;
      text-transform: uppercase;
      margin-bottom: 8px;
    }

    .input-inner {
      position: relative;
      display: flex;
      align-items: center;
    }

    .input-inner i.input-icon {
      position: absolute;
      left: 16px;
      font-size: 1.25rem;
      color: #94a3b8;
      pointer-events: none;
      transition: color 0.2s ease;
    }

    .custom-input-control {
      width: 100%;
      height: 52px;
      padding: 12px 16px 12px 48px;
      font-size: 0.98rem;
      color: #0f172a;
      background: #f8fafc;
      border: 1.5px solid #e2e8f0;
      border-radius: 12px;
      outline: none;
      transition: all 0.25s ease;
    }

    .custom-input-control:focus {
      background: #ffffff;
      border-color: #0284c7;
      box-shadow: 0 0 0 4px rgba(2, 132, 199, 0.12);
    }

    .input-inner:focus-within i.input-icon {
      color: #0284c7;
    }

    .btn-submit-login {
      width: 100%;
      height: 54px;
      background: linear-gradient(135deg, #0284c7 0%, #0369a1 100%);
      color: #ffffff;
      border: none;
      border-radius: 12px;
      font-size: 1.05rem;
      font-weight: 700;
      letter-spacing: 0.5px;
      cursor: pointer;
      box-shadow: 0 10px 20px -5px rgba(2, 132, 199, 0.4);
      transition: all 0.3s ease;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      margin-top: 10px;
    }

    .btn-submit-login:hover {
      background: linear-gradient(135deg, #0369a1 0%, #075985 100%);
      transform: translateY(-2px);
      box-shadow: 0 14px 24px -5px rgba(2, 132, 199, 0.5);
    }

    /* Responsive Breakdown */
    @media (max-width: 992px) {
      .login-container {
        flex-direction: column;
      }
      .hero-side, .form-side {
        flex: 0 0 100%;
        max-width: 100%;
      }
      .hero-side {
        padding: 40px 30px;
      }
      .feature-cards-grid {
        grid-template-columns: 1fr;
      }
      .form-side {
        padding: 40px 25px;
      }
    }
  </style>

  <script type="text/javascript">
    function checkpass() {
      if (document.changepassword.newpassword.value != document.changepassword.confirmpassword.value) {
        alert('New Password and Confirm Password fields do not match.');
        document.changepassword.confirmpassword.focus();
        return false;
      }
      return true;
    }
  </script>
</head>

<body>
  <div class="login-container">
    
    <!-- Left 58% Hero Column -->
    <div class="hero-side">
      <div>
        <div class="brand-header-logos">
          <div class="brand-logo-card">
            <img src="images/pmidc.jpg" alt="PMIDC Logo">
          </div>
          <div class="brand-logo-card">
            <img src="images/logo1.png" alt="Punjab Government Logo">
          </div>
        </div>

        <div class="hero-badge">
          <i class="typcn typcn-key"></i> Password Reset Verified
        </div>

        <h1 class="hero-title">Set Your New Account Password</h1>
        <p class="hero-subtitle">
          Identity verified successfully. Create a strong, new password for your PMIDC Streetlight Portal login account.
        </p>

        <div class="feature-cards-grid">
          <div class="feature-card">
            <div class="feature-icon-wrapper">
              <i class="typcn typcn-shield-check"></i>
            </div>
            <h5>Secure MD5 Hashing</h5>
            <p>Your password is cryptographically protected before saving.</p>
          </div>

          <div class="feature-card">
            <div class="feature-icon-wrapper">
              <i class="typcn typcn-tick-outline"></i>
            </div>
            <h5>Instant Access</h5>
            <p>Log in immediately with your updated password after submitting.</p>
          </div>
        </div>
      </div>

      <div class="pt-4">
        <p class="hero-footer-text">
          © 2026 Punjab Municipal Infrastructure Development Company (PMIDC). All rights reserved.
        </p>
      </div>
    </div>

    <!-- Right 42% Form Column -->
    <div class="form-side">
      <div class="form-wrapper">
        
        <div class="form-header">
          <h2>Create New Password</h2>
          <p>Please enter and confirm your new password</p>
        </div>

        <?php if (!empty($error)): ?>
          <div class="alert alert-danger alert-dismissible fade show mb-4" role="alert" style="border-radius: 12px; font-size: 0.9rem;">
            <i class="typcn typcn-warning-outline mr-1"></i> <strong>Error:</strong> <?php echo $error; ?>
            <button type="button" class="close" data-dismiss="alert" aria-label="Close">
              <span aria-hidden="true">&times;</span>
            </button>
          </div>
        <?php endif; ?>

        <form method="post" name="changepassword" action="resetpassword.php" onsubmit="return checkpass();">
          
          <div class="custom-input-group">
            <label for="newpassword">NEW PASSWORD</label>
            <div class="input-inner">
              <i class="typcn typcn-key input-icon"></i>
              <input type="password" class="custom-input-control" id="newpassword" name="newpassword" placeholder="Enter new password" required>
            </div>
          </div>

          <div class="custom-input-group">
            <label for="confirmpassword">CONFIRM NEW PASSWORD</label>
            <div class="input-inner">
              <i class="typcn typcn-lock-closed input-icon"></i>
              <input type="password" class="custom-input-control" id="confirmpassword" name="confirmpassword" placeholder="Confirm new password" required>
            </div>
          </div>

          <button type="submit" name="submit" class="btn-submit-login">
            <i class="typcn typcn-device-floppy"></i> Save New Password & Login
          </button>

          <div class="text-center mt-4">
            <a href="index.php" class="text-primary font-weight-bold" style="font-size: 0.9rem; text-decoration: none;">
              <i class="typcn typcn-arrow-left-outline"></i> Back to Sign In
            </a>
          </div>
        </form>

      </div>
    </div>

  </div>

  <script src="vendors/js/vendor.bundle.base.js"></script>
  <script src="js/off-canvas.js"></script>
  <script src="js/hoverable-collapse.js"></script>
  <script src="js/template.js"></script>
</body>

</html>
