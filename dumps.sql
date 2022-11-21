create table if not exists parsed_user (
    id serial primary key,
    title text not null unique,
    description text,
    email text,
    phone text,
    subscribers text not null default '0',
    subscriptions text not null default '0'
);

create table if not exists parsed_post (
    id serial primary key,
    title text not null,
    description text,
    last text not null,
    author int not null,
    url text not null unique,

    foreign key (author) references parsed_user(id)
);