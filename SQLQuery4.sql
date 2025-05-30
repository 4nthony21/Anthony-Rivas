create table genre (
id int primary key,
description varchar(50) not null
);

create table type (
id int primary key,
name varchar(50) not null,
description text
);

create table durationType (
id int primary key,
description varchar(50) not null
);

create table country (
id int primary key,
name varchar(50) not null
);

create table rating (
id int primary key,
description varchar(50) not null
);

create table positionType (
id int primary key,
description varchar(50) not null
);

create table people (
id int primary key,
name varchar(500) not null,
positionTypeId int not null,
otherDetails varchar(50)
);

create table show (
id varchar(10) primary key,
typeId int,
title text not null,
date_added date,
release_year varchar(4) not null,
ratingId int,
durationTypeId int,
durationQuantity int,
description text
);

create table listed (
id int not null primary key,
showId varchar(10),
genreId int,
constraint FK_listed_genre foreign key (genreId)
	references genre(id),
constraint FK_listed_show foreign key (showId)
	references show(id)
on delete cascade
on update cascade
);

create table location (
id int not null primary key,
showId varchar(10),
countryId int,
constraint FK_location_country foreign key (countryId)
	references country(id),
constraint FK_location_show foreign key (showId)
	references show(id)
on delete cascade
on update cascade
);

create table casting (
id int not null primary key,
showId varchar(10),
peopleId int,
constraint FK_casting_people foreign key (peopleId)
	references people(id),
constraint FK_casting_show foreign key (showId)
	references show(id)
on delete cascade
on update cascade
);