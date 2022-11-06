-module(jpeg).
-compile(export_all).
-record(marker, {type, size, data}).

%% See EXIF data format description:
% https://www.media.mit.edu/pia/Research/deepview/exif.html

read_start_of_image(<<16#FF:8, 16#D8:8, Img/binary>>) -> Img;
read_start_of_image(_) -> throw(not_start_of_image).

is_end_of_image(<<16#FF:8, 16#D9:8>>) -> true;
is_end_of_image(_) -> false.


hex_dump(Binary) -> hex_dump(Binary, 0).
hex_dump(Binary, I) ->
    if I < size(Binary) ->
            io:format("~.16B ", [binary:at(Binary,I)]),
            hex_dump(Binary,I+1);
       true -> void
    end.

read_marker(Binary) ->
    {Marker,Rest} = split_binary(Binary, 4),
    case Marker of
        <<16#FF:8, MarkerNum:8, DataSize:16/big>> ->
            % io:format("marker ~.16B with size ~p ~n", [MarkerNum, DataSize-2]),
            {Data, Rest1} = split_binary(Rest, DataSize-2),
            %hex_dump(Data),
            {#marker{type=MarkerNum,size=DataSize-2,data=Data}, Rest1};
        _ -> throw({not_at_marker, Marker})
    end.

read_segments(Bin,Acc) ->
    case is_end_of_image(Bin) of
        true -> Acc;
        false ->
            {Marker, Rest} = read_marker(Bin),
            read_segments(Rest, [Marker | Acc])
    end.


read(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Img = read_start_of_image(Bin),
    read_segments(Img,[]).

read_tiff_header(Bin) ->
    case Bin of
        <<"Exif", 0, 0, % EXIF magic
          _Endianness:2/binary, 0, 42, % TIFF magic
          IfdOffset:32,
          Rest/binary>> ->
            {_, IfdData} = split_binary(Rest, IfdOffset-8),
            read_ifds(IfdData);
        _ ->
            hex_dump(element(split_binary(Bin,32), 1)),
            {error, Bin}
    end.


ifd_tag(16#010E) -> image_description;
ifd_tag(16#010F) -> make;
ifd_tag(16#0110) -> model;
ifd_tag(16#0132) -> date_time;
ifd_tag(16#0131) -> software;
ifd_tag(16#8825) -> gps_info;
ifd_tag(16#013C) -> host_computer;
% EXIF 2.3 IFD1 tags
ifd_tag(16#0100) -> image_width;
ifd_tag(16#0101) -> image_height;
ifd_tag(16#0102) -> bits_per_sample;
ifd_tag(16#0103) -> compression;
ifd_tag(16#0106) -> photometric_interpretation;
ifd_tag(16#0111) -> strip_offsets;
ifd_tag(16#0112) -> orientation;
ifd_tag(16#0115) -> samples_per_pixel;
ifd_tag(16#0116) -> rows_per_strip;
ifd_tag(16#0117) -> stripbytecounts;
ifd_tag(16#011A) -> x_resolution;
ifd_tag(16#011B) -> y_resolution;
ifd_tag(16#011C) -> planar_configuration;
ifd_tag(16#0128) -> resolution_unit;
ifd_tag(16#0201) -> jpeg_if_offset;
ifd_tag(16#0202) -> jpeg_if_byte_count;
ifd_tag(16#0211) -> ycbcrcoefficients;
ifd_tag(16#0212) -> ycbcrsubsampling;
ifd_tag(16#0213) -> ycbcrpositioning;
ifd_tag(16#0214) -> reference_black_white;
ifd_tag(X) -> X.

ifd_val(2, Data) ->
    binary_to_list(binary:part(Data, 0, size(Data)-1));
ifd_val(3, <<Short:16/unsigned, _/binary>>) -> Short;
ifd_val(4, <<Long:32/unsigned>>) -> Long;
ifd_val(5, <<Numerator:32/unsigned,Denominator:32/unsigned>> = D) ->
    %io:format("rational: ~p~n", [D]),
    {Numerator,Denominator};
ifd_val(X, Data) -> {type,X,data,Data}.

% read multiple ifd values
ifd_vals(_Type,<<>>,_BytesPerComponent) -> [];
ifd_vals(Type, Data, BytesPerComponent) ->
    {First,Rest} = split_binary(Data, BytesPerComponent),
    [ifd_val(Type,First) | ifd_vals(Type,Rest,BytesPerComponent)].

read_ifd_entries(_, Count, Bin, Map) when Count =:= 0 -> {Map, Bin};
read_ifd_entries(Ifd, Count, Bin, Map) ->
    case Bin of
        <<Tag:16,Type:16,Cnt:32,DataOrOffset:4/binary,Rest/binary>> ->
            %io:format("Got tag: ~p, type: ~p, cnt: ~p at ~p~n", [Tag,Type,Cnt,DataOrOffset]),
            BytesPerComponent = case Type of
                                    1 -> 1; % unsigned byte
                                    2 -> 1; % string
                                    3 -> 2; % unsigned short
                                    4 -> 4; % unsigned long
                                    5 -> 8; % unsigned rational
                                    6 -> 1; % signed byte
                                    7 -> 1; % undefined
                                    8 -> 2; % signed short
                                    9 -> 4; % signed long
                                    10 -> 8; % signed rational
                                    11 -> 4; % single float
                                    12 -> 8 % double float
                                end,
            Size = Cnt * BytesPerComponent,
            % If Size<4 then DataOrOffset is the actual data,
            % otherwise it is an offset to get the data.
            Data = if
                       Size =< 4 -> DataOrOffset;
                       true -> (case DataOrOffset of
                                    <<Offset:32>> -> binary:part(Ifd, Offset-8, Size)
                                end)
                   end,
            TagName = ifd_tag(Tag),
            TagVal = if Cnt > 1 andalso Type /= 2 ->
                             ifd_vals(Type,Data,BytesPerComponent);
                        true -> ifd_val(Type,Data)
                     end,
            Val = case TagName of
                      % if tag is a link to another IFD, read that
                      gps_info -> element(1, read_ifd(element(2, split_binary(Ifd, TagVal-8))));
                      _ -> TagVal
                  end,
            read_ifd_entries(Ifd, Count-1, Rest,
                             maps:put(TagName, Val, Map));
        _ -> {error, invalid_ifd_entry, Count, Bin, Map}
    end.



read_ifd(Bin) ->
    case Bin of
        <<Count:16, Rest/binary>> ->
            read_ifd_entries(Bin, Count, Rest, #{});
        _ -> {error, not_ifd, Bin}
    end.

read_ifds(Bin) ->
    read_ifds(Bin, Bin, #{}).
read_ifds(Whole, Bin, Map) ->
    {IfdMap, Rest} = read_ifd(Bin),
    NewMap = maps:merge(Map,IfdMap),
    case Rest of
        <<0:32, Img/binary>> -> {NewMap, Img};
        <<NextIfd:32, _/binary>> ->
            %% offset from where??
            %io:format("seuraava ~p~n", [NextIfd]),
            read_ifds(Whole, element(2,split_binary(Whole,NextIfd-8)), NewMap)
            %%{seuraava, NextIfd, Map, IfdMap}
    end.

read_header(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    Img = read_start_of_image(Bin),
    {App1,_Rest} = read_marker(Img),
    read_tiff_header(App1#marker.data).
