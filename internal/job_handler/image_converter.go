package jobhandler

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"strings"
)

type ImageConvertRequest struct {
	Data   []byte
	Format string
}

func (h *Handler) ConvertImage(ctx context.Context, req ImageConvertRequest) ([]byte, error) {
	_ = ctx

	if len(req.Data) == 0 {
		return nil, fmt.Errorf("jobhandler: image data is required")
	}
	if strings.TrimSpace(req.Format) == "" {
		return nil, fmt.Errorf("jobhandler: target format is required")
	}

	src, _, err := image.Decode(bytes.NewReader(req.Data))
	if err != nil {
		return nil, err
	}

	return encodeImage(src, req.Format)
}

func normalizeFormat(format string) string {
	return strings.ToLower(strings.TrimSpace(format))
}

func jpegEncode(buf *bytes.Buffer, img image.Image) error {
	return jpeg.Encode(buf, img, &jpeg.Options{Quality: 90})
}

func pngEncode(buf *bytes.Buffer, img image.Image) error {
	return png.Encode(buf, img)
}

func gifEncode(buf *bytes.Buffer, img image.Image) error {
	return gif.Encode(buf, img, nil)
}
