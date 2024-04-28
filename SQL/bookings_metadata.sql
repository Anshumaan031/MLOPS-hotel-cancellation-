CREATE DATABASE IF NOT EXISTS `hotel_datawarehouse` /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `hotel_datawarehouse`;

-- MySQL dump 10.13  Distrib 5.6.13, for Win32 (x86)
--
-- Host: localhost    Database: hotel_datawarehouse
-- ------------------------------------------------------
-- Server version	5.6.15-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

DROP TABLE IF EXISTS `bookings_metadata`;

CREATE TABLE IF NOT EXISTS `bookings_metadata` (
    feature_name VARCHAR(255),
    description VARCHAR(255)
);

INSERT INTO `bookings_metadata` VALUES ('hotel', 'Indicates the type of hotel booked (Resort Hotel or City Hotel)'),
('is_canceled', 'Binary value indicating whether the booking was canceled (1) or not (0). This forms the target variable for our use case'),
('lead_time', 'Number of days in advance the booking was made prior to the arrival date'),
('arrival_date_year', 'Year of the arrival date'),
('arrival_date_month', 'Month of the arrival date'),
('arrival_date_week_number', 'Week number of the year for the arrival date'),
('arrival_date_day_of_month', 'Day of the month for the arrival date'),
('stays_in_weekend_nights', 'Number of weekend nights (Saturday or Sunday) the guest stayed or booked to stay at the hotel'),
('stays_in_week_nights', 'Number of week nights (Monday to Friday) the guest stayed or booked to stay at the hotel'),
('adults', 'Number of adults included in the booking'),
('children', 'Number of children included in the booking'),
('babies', 'Number of babies included in the booking'),
('meal', 'Type of meal booked (e.g., Bed & Breakfast, Half board, Full board)'),
('country', 'Country of origin of the booking'),
('market_segment', 'Market segment designation (e.g., Online TA, Offline TA/TO)'),
('distribution_channel', 'Booking distribution channel (e.g., TA/TO, Direct)'),
('is_repeated_guest', 'Binary value indicating if the booking name was from a repeated guest (1) or not (0)'),
('previous_cancellations', 'Number of previous bookings that were canceled by the customer prior to the current booking'),
('previous_bookings_not_canceled', 'Number of previous bookings not canceled by the customer prior to the current booking'),
('reserved_room_type', 'Code of the room type reserved'),
('assigned_room_type', 'Code for the type of room assigned to the booking'),
('booking_changes', 'Number of changes/amendments made to the booking from the moment it was entered until check-in or cancellation'),
('deposit_type', 'Indication if the customer made a deposit to guarantee the booking'),
('agent', 'ID of the travel agency that made the booking'),
('company', 'ID of the company/entity that made the booking or responsible for paying the booking'),
('days_in_waiting_list', 'Number of days the booking was in the waiting list before confirmation'),
('customer_type', 'Type of booking (e.g., Contract, Group, Transient)'),
('adr', 'Average Daily Rate, calculated by dividing the sum of all lodging transactions by the total number of staying nights'),
('required_car_parking_spaces', 'Number of car parking spaces required by the customer'),
('total_of_special_requests', 'Number of special requests made by the customer'),
('reservation_status', 'Reservation status indicating if it was canceled, checked-out, or a no-show'),
('reservation_status_date', 'Date at which the last status was set');

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2014-04-10




