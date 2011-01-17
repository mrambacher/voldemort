use test;
create table test_store (
  key_ver_hash_ 	bigint not null,
  key_hash_ 		integer not null;
  key_ 				varbinary(200) not null,
  version_ 			varbinary(200) not null,
  value_ 			blob
) ;

create index test_store_key_ver_hash_hash_ on test_store(key_ver_hash_);
create index test_store_key_hash_hash_ on test_store(key_hash_);

