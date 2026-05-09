defmodule NeonFS.Client.Chunker.Pure do
  @moduledoc """
  Pure-Elixir port of the FastCDC v2020 cut algorithm (#797).

  Direct translation of `fastcdc::v2020::cut_gear` from the upstream
  Rust crate (v4.0.1) so the chunk-boundary set is bit-exact with
  `NeonFS.Client.Chunker.Native.nif_chunk_data/3` for the same
  `(min, avg, max)` parameters and Level1 normalization. Equivalence
  is pinned by the property test at
  `test/neon_fs/client/chunker/pure_property_test.exs`.

  The deliverable is the comparison the issue called for, not a drop-
  in replacement: for bulk inputs the BEAM's per-byte ETS / `elem/2`
  loop is roughly 30–50× slower than the Rust path, so the
  `NeonFS.Client.Chunker.Native` NIF stays the default chunker. See
  the PR description for the benchmark numbers.

  ## What's here

    * `cut/2` — find the next cut point in a binary; returns
      `{hash, end_offset}` matching `fastcdc::v2020::FastCDC::cut`.
    * `chunks/2` — iterate `cut/2` over the whole input, returning
      a list of `{hash, offset, length}` tuples.
  """

  import Bitwise

  @typedoc """
  Chunker parameters. `level` is one of `0..3` matching
  `Normalization::Level0..3`.
  """
  @type opts :: %{
          required(:min) => pos_integer(),
          required(:avg) => pos_integer(),
          required(:max) => pos_integer(),
          optional(:level) => 0..3
        }

  @mask_64 0xFFFFFFFFFFFFFFFF

  # Masks-per-target-bits table; the 5-bit padding at the start matches
  # the Rust source so `MASKS[bits + 1]` etc. resolves identically.
  @masks {
    0,
    0,
    0,
    0,
    0,
    0x0000000001804110,
    0x0000000001803110,
    0x0000000018035100,
    0x0000001800035300,
    0x0000019000353000,
    0x0000590003530000,
    0x0000D90003530000,
    0x0000D90103530000,
    0x0000D90303530000,
    0x0000D90313530000,
    0x0000D90F03530000,
    0x0000D90303537000,
    0x0000D90703537000,
    0x0000D90707537000,
    0x0000D91707537000,
    0x0000D91747537000,
    0x0000D91767537000,
    0x0000D93767537000,
    0x0000D93777537000,
    0x0000D93777577000,
    0x0000DB3777577000
  }

  @gear {
    0x3B5D3C7D207E37DC,
    0x784D68BA91123086,
    0xCD52880F882E7298,
    0xEACF8E4E19FDCCA7,
    0xC31F385DFBD1632B,
    0x1D5F27001E25ABE6,
    0x83130BDE3C9AD991,
    0xC4B225676E9B7649,
    0xAA329B29E08EB499,
    0xB67FCBD21E577D58,
    0x0027BAAADA2ACF6B,
    0xE3EF2D5AC73C2226,
    0x0890F24D6ED312B7,
    0xA809E036851D7C7E,
    0xF0A6FE5E0013D81B,
    0x1D026304452CEC14,
    0x03864632648E248F,
    0xCDAACF3DCD92B9B4,
    0xF5E012E63C187856,
    0x8862F9D3821C00B6,
    0xA82F7338750F6F8A,
    0x1E583DC6C1CB0B6F,
    0x7A3145B69743A7F1,
    0xABB20FEE404807EB,
    0xB14B3CFE07B83A5D,
    0xB9DC27898ADB9A0F,
    0x3703F5E91BAA62BE,
    0xCF0BB866815F7D98,
    0x3D9867C41EA9DCD3,
    0x1BE1FA65442BF22C,
    0x14300DA4C55631D9,
    0xE698E9CBC6545C99,
    0x4763107EC64E92A5,
    0xC65821FC65696A24,
    0x76196C064822F0B7,
    0x485BE841F3525E01,
    0xF652BC9C85974FF5,
    0xCAD8352FACE9E3E9,
    0x2A6ED1DCEB35E98E,
    0xC6F483BADC11680F,
    0x3CFD8C17E9CF12F1,
    0x89B83C5E2EA56471,
    0xAE665CFD24E392A9,
    0xEC33C4E504CB8915,
    0x3FB9B15FC9FE7451,
    0xD7FD1FD1945F2195,
    0x31ADE0853443EFD8,
    0x255EFC9863E1E2D2,
    0x10EAB6008D5642CF,
    0x46F04863257AC804,
    0xA52DC42A789A27D3,
    0xDAAADF9CE77AF565,
    0x6B479CD53D87FEBB,
    0x6309E2D3F93DB72F,
    0xC5738FFBAA1FF9D6,
    0x6BD57F3F25AF7968,
    0x67605486D90D0A4A,
    0xE14D0B9663BFBDAE,
    0xB7BBD8D816EB0414,
    0xDEF8A4F16B35A116,
    0xE7932D85AAAFFED6,
    0x08161CBAE90CFD48,
    0x855507BEB294F08B,
    0x91234EA6FFD399B2,
    0xAD70CF4B2435F302,
    0xD289A97565BC2D27,
    0x8E558437FFCA99DE,
    0x96D2704B7115C040,
    0x0889BBCDFC660E41,
    0x5E0D4E67DC92128D,
    0x72A9F8917063ED97,
    0x438B69D409E016E3,
    0xDF4FED8A5D8A4397,
    0x00F41DCF41D403F7,
    0x4814EB038E52603F,
    0x9DAFBACC58E2D651,
    0xFE2F458E4BE170AF,
    0x4457EC414DF6A940,
    0x06E62F1451123314,
    0xBD1014D173BA92CC,
    0xDEF318E25ED57760,
    0x9FEA0DE9DFCA8525,
    0x459DE1E76C20624B,
    0xAEEC189617E2D666,
    0x126A2C06AB5A83CB,
    0xB1321532360F6132,
    0x65421503DBB40123,
    0x2D67C287EA089AB3,
    0x6C93BFF5A56BD6B6,
    0x4FFB2036CAB6D98D,
    0xCE7B785B1BE7AD4F,
    0xEDB42EF6189FD163,
    0xDC905288703988F6,
    0x365F9C1D2C691884,
    0xC640583680D99BFE,
    0x3CD4624C07593EC6,
    0x7F1EA8D85D7C5805,
    0x014842D480B57149,
    0x0B649BCB5A828688,
    0xBCD5708ED79B18F0,
    0xE987C862FBD2F2F0,
    0x982731671F0CD82C,
    0xBAF13E8B16D8C063,
    0x8EA3109CBD951BBA,
    0xD141045BFB385CAD,
    0x2ACBC1A0AF1F7D30,
    0xE6444D89DF03BFDF,
    0xA18CC771B8188FF9,
    0x9834429DB01C39BB,
    0x214ADD07FE086A1F,
    0x8F07C19B1F6B3FF9,
    0x56A297B1BF4FFE55,
    0x94D558E493C54FC7,
    0x40BFC24C764552CB,
    0x931A706F8A8520CB,
    0x32229D322935BD52,
    0x2560D0F5DC4FEFAF,
    0x9DBCC48355969BB6,
    0x0FD81C3985C0B56A,
    0xE03817E1560F2BDA,
    0xC1BB4F81D892B2D5,
    0xB0C4864F4E28D2D7,
    0x3ECC49F9D9D6C263,
    0x51307E99B52BA65E,
    0x8AF2B688DA84A752,
    0xF5D72523B91B20B6,
    0x6D95FF1FF4634806,
    0x562F21555458339A,
    0xC0CE47F889336346,
    0x487823E5089B40D8,
    0xE4727C7EBC6D9592,
    0x5A8F7277E94970BA,
    0xFCA2F406B1C8BB50,
    0x5B1F8A95F1791070,
    0xD304AF9FC9028605,
    0x5440AB7FC930E748,
    0x312D25FBCA2AB5A1,
    0x10F4A4B234A4D575,
    0x90301D55047E7473,
    0x3B6372886C61591E,
    0x293402B77C444E06,
    0x451F34A4D3E97DD7,
    0x3158D814D81BC57B,
    0x034942425B9BDA69,
    0xE2032FF9E532D9BB,
    0x62AE066B8B2179E5,
    0x9545E10C2F8D71D8,
    0x7FF7483EB2D23FC0,
    0x00945FCEBDC98D86,
    0x8764BBBE99B26CA2,
    0x1B1EC62284C0BFC3,
    0x58E0FCC4F0AA362B,
    0x5F4ABEFA878D458D,
    0xFD74AC2F9607C519,
    0xA4E3FB37DF8CBFA9,
    0xBF697E43CAC574E5,
    0x86F14A3F68F4CD53,
    0x24A23D076F1CE522,
    0xE725CD8048868CC8,
    0xBF3C729EB2464362,
    0xD8F6CD57B3CC1ED8,
    0x6329E52425541577,
    0x62AA688AD5AE1AC0,
    0x0A242566269BF845,
    0x168B1A4753ACA74B,
    0xF789AFEFFF2E7E3C,
    0x6C3362093B6FCCDB,
    0x4CE8F50BD28C09B2,
    0x006A2DB95AE8AA93,
    0x975B0D623C3D1A8C,
    0x18605D3935338C5B,
    0x5BB6F6136CAD3C71,
    0x0F53A20701F8D8A6,
    0xAB8C5AD2E7E93C67,
    0x40B5AC5127ACAA29,
    0x8C7BF63C2075895F,
    0x78BD9F7E014A805C,
    0xB2C9E9F4F9C8C032,
    0xEFD6049827EB91F3,
    0x2BE459F482C16FBD,
    0xD92CE0C5745AAA8C,
    0x0AAA8FB298D965B9,
    0x2B37F92C6C803B15,
    0x8C54A5E94E0F0E78,
    0x95F9B6E90C0A3032,
    0xE7939FAA436C7874,
    0xD16BFE8F6A8A40C9,
    0x44982B86263FD2FA,
    0xE285FB39F984E583,
    0x779A8DF72D7619D3,
    0xF2D79A8DE8D5DD1E,
    0xD1037354D66684E2,
    0x004C82A4E668A8E5,
    0x31D40A7668B044E6,
    0xD70578538BD02C11,
    0xDB45431078C5F482,
    0x977121BB7F6A51AD,
    0x73D5CCBD34EFF8DD,
    0xE437A07D356E17CD,
    0x47B2782043C95627,
    0x9FB251413E41D49A,
    0xCCD70B60652513D3,
    0x1C95B31E8A1B49B2,
    0xCAE73DFD1BCB4C1B,
    0x34D98331B1F5B70F,
    0x784E39F22338D92F,
    0x18613D4A064DF420,
    0xF1D8DAE25F0BCEBE,
    0x33F77C15AE855EFC,
    0x3C88B3B912EB109C,
    0x956A2EC96BAFEEA5,
    0x1AA005B5E0AD0E87,
    0x5500D70527C4BB8E,
    0xE36C57196421CC44,
    0x13C4D286CC36EE39,
    0x5654A23D818B2A81,
    0x77B1DC13D161ABDC,
    0x734F44DE5F8D5EB5,
    0x60717E174A6C89A2,
    0xD47D9649266A211E,
    0x5B13A4322BB69E90,
    0xF7669609F8B5FC3C,
    0x21E6AC55BEDCDAC9,
    0x9B56B62B61166DEA,
    0xF48F66B939797E9C,
    0x35F332F9C0E6AE9A,
    0xCC733F6A9A878DB0,
    0x3DA161E41CC108C2,
    0xB7D74AE535914D51,
    0x4D493B0B11D36469,
    0xCE264D1DFBA9741A,
    0xA9D1F2DC7436DC06,
    0x70738016604C2A27,
    0x231D36E96E93F3D5,
    0x7666881197838D19,
    0x4A2A83090AAAD40C,
    0xF1E761591668B35D,
    0x7363236497F730A7,
    0x301080E37379DD4D,
    0x502DEA2971827042,
    0xC2C5EB858F32625F,
    0x786AFB9EDFAFBDFF,
    0xDAEE0D868490B2A4,
    0x617366B3268609F6,
    0xAE0E35A0FE46173E,
    0xD1A07DE93E824F11,
    0x079B8B115EA4CCA8,
    0x93A99274558FAEBB,
    0xFB1E6E22E08A03B3,
    0xEA635FDBA3698DD0,
    0xCF53659328503A5C,
    0xCDE3B31E6FD5D780,
    0x8E3E4221D3614413,
    0xEF14D0D86BF1A22C,
    0xE1D830D3F16C5DDB,
    0xAABD2B2A451504E1
  }

  @gear_ls {
    0x76BA78FA40FC6FB8,
    0xF09AD1752224610C,
    0x9AA5101F105CE530,
    0xD59F1C9C33FB994E,
    0x863E70BBF7A2C656,
    0x3ABE4E003C4B57CC,
    0x062617BC7935B322,
    0x89644ACEDD36EC92,
    0x54653653C11D6932,
    0x6CFF97A43CAEFAB0,
    0x004F7555B4559ED6,
    0xC7DE5AB58E78444C,
    0x1121E49ADDA6256E,
    0x5013C06D0A3AF8FC,
    0xE14DFCBC0027B036,
    0x3A04C6088A59D828,
    0x070C8C64C91C491E,
    0x9B559E7B9B257368,
    0xEBC025CC7830F0AC,
    0x10C5F3A70438016C,
    0x505EE670EA1EDF14,
    0x3CB07B8D839616DE,
    0xF4628B6D2E874FE2,
    0x57641FDC80900FD6,
    0x629679FC0F7074BA,
    0x73B84F1315B7341E,
    0x6E07EBD23754C57C,
    0x9E1770CD02BEFB30,
    0x7B30CF883D53B9A6,
    0x37C3F4CA8857E458,
    0x28601B498AAC63B2,
    0xCD31D3978CA8B932,
    0x8EC620FD8C9D254A,
    0x8CB043F8CAD2D448,
    0xEC32D80C9045E16E,
    0x90B7D083E6A4BC02,
    0xECA579390B2E9FEA,
    0x95B06A5F59D3C7D2,
    0x54DDA3B9D66BD31C,
    0x8DE90775B822D01E,
    0x79FB182FD39E25E2,
    0x137078BC5D4AC8E2,
    0x5CCCB9FA49C72552,
    0xD86789CA0997122A,
    0x7F7362BF93FCE8A2,
    0xAFFA3FA328BE432A,
    0x635BC10A6887DFB0,
    0x4ABDF930C7C3C5A4,
    0x21D56C011AAC859E,
    0x8DE090C64AF59008,
    0x4A5B8854F1344FA6,
    0xB555BF39CEF5EACA,
    0xD68F39AA7B0FFD76,
    0xC613C5A7F27B6E5E,
    0x8AE71FF7543FF3AC,
    0xD7AAFE7E4B5EF2D0,
    0xCEC0A90DB21A1494,
    0xC29A172CC77F7B5C,
    0x6F77B1B02DD60828,
    0xBDF149E2D66B422C,
    0xCF265B0B555FFDAC,
    0x102C3975D219FA90,
    0x0AAA0F7D6529E116,
    0x22469D4DFFA73364,
    0x5AE19E96486BE604,
    0xA51352EACB785A4E,
    0x1CAB086FFF9533BC,
    0x2DA4E096E22B8080,
    0x1113779BF8CC1C82,
    0xBC1A9CCFB924251A,
    0xE553F122E0C7DB2E,
    0x8716D3A813C02DC6,
    0xBE9FDB14BB14872E,
    0x01E83B9E83A807EE,
    0x9029D6071CA4C07E,
    0x3B5F7598B1C5ACA2,
    0xFC5E8B1C97C2E15E,
    0x88AFD8829BED5280,
    0x0DCC5E28A2246628,
    0x7A2029A2E7752598,
    0xBDE631C4BDAAEEC0,
    0x3FD41BD3BF950A4A,
    0x8B3BC3CED840C496,
    0x5DD8312C2FC5ACCC,
    0x24D4580D56B50796,
    0x62642A646C1EC264,
    0xCA842A07B7680246,
    0x5ACF850FD4113566,
    0xD9277FEB4AD7AD6C,
    0x9FF6406D956DB31A,
    0x9CF6F0B637CF5A9E,
    0xDB685DEC313FA2C6,
    0xB920A510E07311EC,
    0x6CBF383A58D23108,
    0x8C80B06D01B337FC,
    0x79A8C4980EB27D8C,
    0xFE3D51B0BAF8B00A,
    0x029085A9016AE292,
    0x16C93796B5050D10,
    0x79AAE11DAF3631E0,
    0xD30F90C5F7A5E5E0,
    0x304E62CE3E19B058,
    0x75E27D162DB180C6,
    0x1D4621397B2A3774,
    0xA28208B7F670B95A,
    0x559783415E3EFA60,
    0xCC889B13BE077FBE,
    0x43198EE370311FF2,
    0x3068853B60387376,
    0x4295BA0FFC10D43E,
    0x1E0F83363ED67FF2,
    0xAD452F637E9FFCAA,
    0x29AAB1C9278A9F8E,
    0x817F8498EC8AA596,
    0x2634E0DF150A4196,
    0x64453A64526B7AA4,
    0x4AC1A1EBB89FDF5E,
    0x3B798906AB2D376C,
    0x1FB038730B816AD4,
    0xC0702FC2AC1E57B4,
    0x83769F03B12565AA,
    0x61890C9E9C51A5AE,
    0x7D9893F3B3AD84C6,
    0xA260FD336A574CBC,
    0x15E56D11B5094EA4,
    0xEBAE4A477236416C,
    0xDB2BFE3FE8C6900C,
    0xAC5E42AAA8B06734,
    0x819C8FF11266C68C,
    0x90F047CA113681B0,
    0xC8E4F8FD78DB2B24,
    0xB51EE4EFD292E174,
    0xF945E80D639176A0,
    0xB63F152BE2F220E0,
    0xA6095F3F92050C0A,
    0xA88156FF9261CE90,
    0x625A4BF794556B42,
    0x21E949646949AAEA,
    0x20603AAA08FCE8E6,
    0x76C6E510D8C2B23C,
    0x5268056EF8889C0C,
    0x8A3E6949A7D2FBAE,
    0x62B1B029B0378AF6,
    0x06928484B737B4D2,
    0xC4065FF3CA65B376,
    0xC55C0CD71642F3CA,
    0x2A8BC2185F1AE3B0,
    0xFFEE907D65A47F80,
    0x0128BF9D7B931B0C,
    0x0EC9777D3364D944,
    0x363D8C4509817F86,
    0xB1C1F989E1546C56,
    0xBE957DF50F1A8B1A,
    0xFAE9585F2C0F8A32,
    0x49C7F66FBF197F52,
    0x7ED2FC87958AE9CA,
    0x0DE2947ED1E99AA6,
    0x49447A0EDE39CA44,
    0xCE4B9B00910D1990,
    0x7E78E53D648C86C4,
    0xB1ED9AAF67983DB0,
    0xC653CA484AA82AEE,
    0xC554D115AB5C3580,
    0x14484ACC4D37F08A,
    0x2D16348EA7594E96,
    0xEF135FDFFE5CFC78,
    0xD866C41276DF99B6,
    0x99D1EA17A5181364,
    0x00D45B72B5D15526,
    0x2EB61AC4787A3518,
    0x30C0BA726A6718B6,
    0xB76DEC26D95A78E2,
    0x1EA7440E03F1B14C,
    0x5718B5A5CFD278CE,
    0x816B58A24F595452,
    0x18F7EC7840EB12BE,
    0xF17B3EFC029500B8,
    0x6593D3E9F3918064,
    0xDFAC09304FD723E6,
    0x57C8B3E90582DF7A,
    0xB259C18AE8B55518,
    0x15551F6531B2CB72,
    0x566FF258D900762A,
    0x18A94BD29C1E1CF0,
    0x2BF36DD218146064,
    0xCF273F5486D8F0E8,
    0xA2D7FD1ED5148192,
    0x8930570C4C7FA5F4,
    0xC50BF673F309CB06,
    0xEF351BEE5AEC33A6,
    0xE5AF351BD1ABBA3C,
    0xA206E6A9ACCD09C4,
    0x00990549CCD151CA,
    0x63A814ECD16089CC,
    0xAE0AF0A717A05822,
    0xB68A8620F18BE904,
    0x2EE24376FED4A35A,
    0xE7AB997A69DFF1BA,
    0xC86F40FA6ADC2F9A,
    0x8F64F0408792AC4E,
    0x3F64A2827C83A934,
    0x99AE16C0CA4A27A6,
    0x392B663D14369364,
    0x95CE7BFA37969836,
    0x69B3066363EB6E1E,
    0xF09C73E44671B25E,
    0x30C27A940C9BE840,
    0xE3B1B5C4BE179D7C,
    0x67EEF82B5D0ABDF8,
    0x7911677225D62138,
    0x2AD45D92D75FDD4A,
    0x35400B6BC15A1D0E,
    0xAA01AE0A4F89771C,
    0xC6D8AE32C8439888,
    0x2789A50D986DDC72,
    0xACA9447B03165502,
    0xEF63B827A2C357B8,
    0xE69E89BCBF1ABD6A,
    0xC0E2FC2E94D91344,
    0xA8FB2C924CD4423C,
    0xB6274864576D3D20,
    0xEECD2C13F16BF878,
    0x43CD58AB7DB9B592,
    0x36AD6C56C22CDBD4,
    0xE91ECD7272F2FD38,
    0x6BE665F381CD5D34,
    0x98E67ED5350F1B60,
    0x7B42C3C839821184,
    0x6FAE95CA6B229AA2,
    0x9A92761623A6C8D2,
    0x9C4C9A3BF752E834,
    0x53A3E5B8E86DB80C,
    0xE0E7002CC098544E,
    0x463A6DD2DD27E7AA,
    0xECCD10232F071A32,
    0x945506121555A818,
    0xE3CEC2B22CD166BA,
    0xE6C646C92FEE614E,
    0x602101C6E6F3BA9A,
    0xA05BD452E304E084,
    0x858BD70B1E64C4BE,
    0xF0D5F73DBF5F7BFE,
    0xB5DC1B0D09216548,
    0xC2E6CD664D0C13EC,
    0x5C1C6B41FC8C2E7C,
    0xA340FBD27D049E22,
    0x0F371622BD499950,
    0x275324E8AB1F5D76,
    0xF63CDC45C1140766,
    0xD4C6BFB746D31BA0,
    0x9EA6CB2650A074B8,
    0x9BC7663CDFABAF00,
    0x1C7C8443A6C28826,
    0xDE29A1B0D7E34458,
    0xC3B061A7E2D8BBB6,
    0x557A56548A2A09C2
  }

  @doc """
  Find the next chunk cut point in `source` for the given chunker
  parameters.

  Returns `{hash, end_offset}`. When `byte_size(source) <= min`, the
  return is `{0, byte_size(source)}` — the source is too small for a
  cut point, the caller treats it as a single chunk.

  Bit-exact with the upstream `fastcdc::v2020::cut_gear` for
  `Normalization::Level1` (the FastCDC default).
  """
  @spec cut(binary(), opts()) :: {non_neg_integer(), non_neg_integer()}
  def cut(source, %{min: min, avg: avg, max: max} = opts) when is_binary(source) do
    level = Map.get(opts, :level, 1)
    {mask_s, mask_l} = compute_masks(avg, level)
    masks = {mask_s, mask_l, mask_s <<< 1, mask_l <<< 1}
    cut_gear(source, min, avg, max, masks)
  end

  @doc """
  Walk `source` end-to-end, emitting `[{hash, offset, length}, ...]`
  for every chunk the algorithm produces. Output is byte-for-byte
  identical to `NeonFS.Client.Chunker.Native.nif_chunk_data(source,
  "fastcdc", avg)` modulo the SHA-256 hash field, which the NIF
  computes on the chunk bytes.
  """
  @spec chunks(binary(), opts()) :: [{non_neg_integer(), non_neg_integer(), non_neg_integer()}]
  def chunks(source, opts) when is_binary(source) do
    do_chunks(source, opts, 0, [])
  end

  defp do_chunks(<<>>, _opts, _offset, acc), do: Enum.reverse(acc)

  defp do_chunks(source, opts, offset, acc) do
    {hash, length} = cut(source, opts)

    if length == 0 do
      Enum.reverse(acc)
    else
      tail = binary_part(source, length, byte_size(source) - length)
      do_chunks(tail, opts, offset + length, [{hash, offset, length} | acc])
    end
  end

  ## Internal — masks

  defp compute_masks(avg, level) when level in 0..3 do
    bits = log2_round(avg)
    mask_s = elem(@masks, bits + level)
    mask_l = elem(@masks, bits - level)
    {mask_s, mask_l}
  end

  # Rounded base-2 logarithm; matches Rust's
  # `(value as f64).log2().round() as u32`.
  defp log2_round(value) do
    :math.log2(value) |> Float.round() |> trunc()
  end

  ## Internal — cut_gear

  # Mask 4-tuple shape: `{mask_s, mask_l, mask_s_ls, mask_l_ls}`. Bundling
  # them keeps the recursion arity below the credo limit.
  defp cut_gear(source, min, avg, max, masks) do
    src_size = byte_size(source)

    if src_size <= min do
      {0, src_size}
    else
      remaining = min(src_size, max)
      center = min(remaining, avg)

      cut_short(source, div(min, 2), div(center, 2), div(remaining, 2), remaining, 0, masks)
    end
  end

  # Short-region loop: index ∈ [min/2, center/2). Uses mask_s / mask_s_ls.
  defp cut_short(source, index, center_half, rem_half, remaining, hash, masks)
       when index < center_half do
    {mask_s, _mask_l, mask_s_ls, _mask_l_ls} = masks
    a = index * 2
    byte_a = :binary.at(source, a)
    h1 = (hash <<< 2) + elem(@gear_ls, byte_a) &&& @mask_64

    if (h1 &&& mask_s_ls) == 0 do
      {h1, a}
    else
      byte_b = :binary.at(source, a + 1)
      h2 = h1 + elem(@gear, byte_b) &&& @mask_64

      if (h2 &&& mask_s) == 0 do
        {h2, a + 1}
      else
        cut_short(source, index + 1, center_half, rem_half, remaining, h2, masks)
      end
    end
  end

  defp cut_short(source, index, _center_half, rem_half, remaining, hash, masks) do
    cut_long(source, index, rem_half, remaining, hash, masks)
  end

  # Long-region loop: index ∈ [center/2, remaining/2). Uses mask_l / mask_l_ls.
  defp cut_long(source, index, rem_half, remaining, hash, masks)
       when index < rem_half do
    {_mask_s, mask_l, _mask_s_ls, mask_l_ls} = masks
    a = index * 2
    byte_a = :binary.at(source, a)
    h1 = (hash <<< 2) + elem(@gear_ls, byte_a) &&& @mask_64

    if (h1 &&& mask_l_ls) == 0 do
      {h1, a}
    else
      byte_b = :binary.at(source, a + 1)
      h2 = h1 + elem(@gear, byte_b) &&& @mask_64

      if (h2 &&& mask_l) == 0 do
        {h2, a + 1}
      else
        cut_long(source, index + 1, rem_half, remaining, h2, masks)
      end
    end
  end

  defp cut_long(_source, _index, _rem_half, remaining, hash, _masks) do
    # Pathological data (all zeros, etc.) and odd-byte tail-of-stream:
    # no cut found in `[center/2, remaining/2)`, fall back to the
    # largest chunk allowed. Returns `remaining` directly (not
    # `rem_half * 2`) so odd-byte trailing chunks aren't truncated.
    {hash, remaining}
  end
end
