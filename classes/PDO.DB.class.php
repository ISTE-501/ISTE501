<?php
class DB{
    private $dbh;

    function __construct(){

        try{
        $this->dbh = new PDO("mysql:host={$_SERVER['DB_SERVER']}; dbname={$_SERVER['DB']}",$_SERVER['DB_USER'],$_SERVER['DB_PASSWORD']);
        $this->dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        }catch(PDOException $pe){
            echo $pe->getMessage();
        }

    }
    
    function getAllUsers(){
            $data = [];
            try{
                include "Users.class.php";
                $stmt = $this->dbh->prepare("SELECT * FROM users");
                $stmt->execute();
                $stmt->setFetchMode(PDO::FETCH_CLASS, "User");

                while($attendee = $stmt->fetch()){
                    $data[] = $user;
                }
            }catch(PDOException $e){
                echo $e->getMessage();

            }
            return $data;
        }

    function findUser ($login, $password) {
        $data = [];
        try{
            require_once "Users.class.php";
            $stmt = $this->dbh->prepare("SELECT a.id, a.name, a.role FROM users a WHERE a.name=:login AND a.password = :password");
            
            $stmt->execute([
                "login" => $login,
                "password" => $password
            ]);
            $stmt->setFetchMode(PDO::FETCH_CLASS, "Users");
            
            $data  = $stmt->fetchAll();
            if (count($data) != 1) return null;
            return $data[0];
        }catch(PDOException $e){
            echo $e->getMessage();
        }
    }
}

?>