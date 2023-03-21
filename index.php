<?php

session_name("session");
session_start();

$username = "";
$role    = "";


if (isset($_POST['username'])) {
    require_once "classes/PDO.DB.class.php";

    $username = $_POST['username'];
    $password = $_POST['password'];  
    
    $_SESSION["loggedIn"] = false;

    if (empty($password)) {
        header("Location: failed.php");
        return;
    }

    $password = hash('sha256', $password);
       

    $db = new DB();

    $user = $db->findUser($username, $password);
    
    if (is_null($user)) { 
        header("Location: failed.php");
        return;
    }

    $_SESSION["loggedIn"] = true;    
    $_SESSION["role"] = $user->getRole(); //["rolename"];      
    $_SESSION["count"] = 1;
    $_SESSION["username"] = $username;
    $_SESSION["userid"] = $user->getID();
    
    //set session cookie
    $expire = time() + 600;
    $path = "/~sdg3149/";
    $domain = "solace.ist.rit.edu";
    $secure = false;
    setcookie(
        "loggedIn",
        date("l jS \of F Y h:i A"),
        $expire,
        $path,
        $domain,
        $secure
    );

    //header("Location: admin.php");    
}

if(isset($_SESSION['loggedIn']) && isset($_SESSION['role'])) {
  if($_SESSION['role'] == 'p') {
    header("Location: prof/index.php");
    return;
  }
  else if($_SESSION['role'] == 't'){
    header("Location: TA/index.php");
    return;
  }
} else { //render html form below

?>

</html>

<!DOCTYPE html>
<html lang="en">
  <head>
    <title>RIT Server Observer Log-In</title>
    <meta property="og:title" content="Male Motherly Swan" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta charset="utf-8" />
    <meta property="twitter:card" content="summary_large_image" />

    <style data-tag="reset-style-sheet">
      html {  line-height: 1.15;}body {  margin: 0;}* {  box-sizing: border-box;  border-width: 0;  border-style: solid;}p,li,ul,pre,div,h1,h2,h3,h4,h5,h6,figure,blockquote,figcaption {  margin: 0;  padding: 0;}button {  background-color: transparent;}button,input,optgroup,select,textarea {  font-family: inherit;  font-size: 100%;  line-height: 1.15;  margin: 0;}button,select {  text-transform: none;}button,[type="button"],[type="reset"],[type="submit"] {  -webkit-appearance: button;}button::-moz-focus-inner,[type="button"]::-moz-focus-inner,[type="reset"]::-moz-focus-inner,[type="submit"]::-moz-focus-inner {  border-style: none;  padding: 0;}button:-moz-focus,[type="button"]:-moz-focus,[type="reset"]:-moz-focus,[type="submit"]:-moz-focus {  outline: 1px dotted ButtonText;}a {  color: inherit;  text-decoration: inherit;}input {  padding: 2px 4px;}img {  display: block;}html { scroll-behavior: smooth  }
    </style>
    <style data-tag="default-style-sheet">
      html {
        font-family: Inter;
        font-size: 16px;
      }

      body {
        font-weight: 400;
        font-style:normal;
        text-decoration: none;
        text-transform: none;
        letter-spacing: normal;
        line-height: 1.15;
        color: var(--dl-color-gray-black);
        background-color: var(--dl-color-gray-white);

      }
    </style>
    <link
      rel="stylesheet"
      href="https://fonts.googleapis.com/css2?family=Poppins:ital,wght@0,100;0,200;0,300;0,400;0,500;0,600;0,700;0,800;0,900;1,100;1,200;1,300;1,400;1,500;1,600;1,700;1,800;1,900&amp;display=swap"
      data-tag="font"
    />
    <link
      rel="stylesheet"
      href="https://fonts.googleapis.com/css2?family=Inter:wght@100;200;300;400;500;600;700;800;900&amp;display=swap"
      data-tag="font"
    />
    <link rel="stylesheet" href="./style.css" />
  </head>
  <body>
    <div>
      <link href="./home.css" rel="stylesheet" />

      <div class="home-container">
        <div class="home-professor-dashboard">
          <div class="home-header3">
            <div class="home-logo">
              <img
                src="public/playground_assets/logoi217-979-200h.png"
                alt="LogoI217"
                class="home-logo1"
              />
              <img
                src="public/playground_assets/line45i217-zjjs.svg"
                alt="Line45I217"
                class="home-line45"
              />
              <span class="home-text">
                <span class="home-text01">
                  <span>GCCIS</span>
                  <br />
                  <span></span>
                </span>
                <span>Solace</span>
              </span>
            </div>
          </div>
          <div class="home-section">
            <div class="home-widgets">
              <div class="home-logo2">
                <img
                  src="public/playground_assets/newritlogo1rgb01i217-fbik-200h.png"
                  alt="newRITlogo1RGB01I217"
                  class="home-new-r-tlogo1rgb01"
                />
                <img
                  src="public/playground_assets/line45i217-z61j.svg"
                  alt="Line45I217"
                  class="home-line451"
                />
                <span class="home-text06">
                  <span class="home-text07">
                    <span>GCCIS</span>
                    <br />
                    <span></span>
                  </span>
                  <span>Solace</span>
                </span>
              </div>
              <form method="post" action="index.php" class="home-frame105">
                <div class="home-frame103">
                  <span class="home-text12"><span>Username</span></span>
                  <input class="home-text12" type="text" name="username" />
                </div>
                <div class="home-frame102">
                  <span class="home-text14"><span>Password</span></span>
                  <input class="home-text14" type="password" name="password" />
                </div>
                <div class="home-frame106">
                  <span class="home-text16"><span>Forgot password?</span></span>
                  <span class="home-text18"><span>Register</span></span>
                  <div class="home-frame1051">
                    <!--<span class="home-text20"><span>Login</span></span>-->
                    <button class="home-text20" type="submit" name="login_user">Login</button>
                  </div>
                </div>
              </form>
            </div>
          </div>
        </div>
      </div>
    </div>
    <?php } //end else ?>
    <script src="https://code.jquery.com/jquery-3.6.3.min.js" integrity="sha256-pvPw+upLPUjgMXY0G+8O0xUf+/Im1MZjXxxgOcBQBXU=" crossorigin="anonymous"></script>
    <script src="js/scripts.js"></script>
  </body>
</html>


