use ExtUtils::MakeMaker;
use ExtUtils::Depends;
use ExtUtils::PkgConfig;

my %raptor = ExtUtils::PkgConfig->find('raptor2');
my $deps = ExtUtils::Depends->new('AtteanX::Store::MemoryTripleStore', 'XS::Object::Magic');
$deps->set_inc($raptor{cflags});
$deps->set_libs($raptor{libs});

my $inc_files = join(' ', glob '*.h');
my $src_files = join(' ', glob '*.c');
my $xs_files  = join(' ', glob '*.xs');

my %WriteMakefileArgs	= (
    NAME => 'AtteanX::Store::MemoryTripleStore',
    VERSION => '0.001',
    $deps->get_makefile_vars,
    MIN_PERL_VERSION	=> 5.014,
    PREREQ_PM => {
    	'Moose'						=> 0,
    	'Params::Util'				=> 0,
    	'Attean'					=> 0,
    	'XS::Object::Magic'			=> 0,
    	'Moo'						=> 0,
    	'Type::Tiny'				=> 0,
    },
);

$WriteMakefileArgs{OBJECT}	.= join(' ', split(' ', $WriteMakefileArgs{OBJECT}), "triplestore.o", "avl.o", "MemoryTripleStore.o");
$WriteMakefileArgs{depend}{'MemoryTripleStore.xs'} = "avl.o triplestore.o";
$WriteMakefileArgs{depend}{'triplestore.c'} = "triplestore.h avl.h avl.c";
$WriteMakefileArgs{depend}{'avl.c'} = "avl.h";

WriteMakefile( %WriteMakefileArgs );
