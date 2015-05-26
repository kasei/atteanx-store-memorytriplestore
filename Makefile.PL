use ExtUtils::MakeMaker;
use ExtUtils::Depends;
use ExtUtils::PkgConfig;

my %raptor	= ExtUtils::PkgConfig->find('raptor2');
my %pcre	= ExtUtils::PkgConfig->find('libpcre');
my $deps = ExtUtils::Depends->new('AtteanX::Store::MemoryTripleStore', 'XS::Object::Magic');

$deps->set_inc($raptor{cflags}, $pcre{cflags});
$deps->set_libs($raptor{libs}, $pcre{libs});

my $inc_files = join(' ', glob '*.h');
my $src_files = join(' ', glob '*.c');
my $xs_files  = join(' ', glob '*.xs');

my %WriteMakefileArgs	= (
    NAME							=> 'AtteanX::Store::MemoryTripleStore',
    VERSION 						=> '0.000_01',
    LICENSE							=> 'gpl_2',
    MIN_PERL_VERSION				=> 5.014,
    PREREQ_PM => {
    	'Attean'					=> 0.004,
    	'Moose'						=> 0,
    	'Params::Util'				=> 0,
    	'XS::Object::Magic'			=> 0,
    	'Moo'						=> 0,
    	'Type::Tiny'				=> 0,
    },
    TEST_REQUIRES => {
    	'File::Temp'				=> 0,
    	'Test::Roo'					=> 0,
    	'Test::Exception'			=> 0,
    	'Test::More'				=> 0,
    },
    CC								=> 'clang',
    $deps->get_makefile_vars,
);

$WriteMakefileArgs{OBJECT}	.= join(' ', split(' ', $WriteMakefileArgs{OBJECT}), "triplestore.o", "avl.o", "MemoryTripleStore.o");
$WriteMakefileArgs{depend}{'MemoryTripleStore.xs'} = "avl.o triplestore.o";
$WriteMakefileArgs{depend}{'triplestore.c'} = "triplestore.h avl.h avl.c";
$WriteMakefileArgs{depend}{'avl.c'} = "avl.h";

my @CCFLAGS	= ('-g', '-fblocks');
my @LIBS	= '-lBlocksRuntime';

$WriteMakefileArgs{CCFLAGS}	.= join(' ', '', @CCFLAGS);
$WriteMakefileArgs{LIBS}	.= join(' ', '', @LIBS);

WriteMakefile(
	%WriteMakefileArgs,
);
